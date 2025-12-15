import argparse
import socket
import uuid
import time
import traceback
from typing import Optional, Tuple

from app.core.queue import lease_one_task, heartbeat, mark_running, mark_failed
from app.core.db import get_conn
from app.backend.slurm.client import submit_demo_sleep, get_job_state

LEASE_SECONDS = 120
LEASED_BY = f"{socket.gethostname()}:{uuid.uuid4()}"


TASK_STATUS_SQL = """
SELECT status, error, backend_job_id
FROM tasks
WHERE id = %s::uuid
"""


def _get_task_status(task_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Возвращает (status, error, backend_job_id) из БД."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(TASK_STATUS_SQL, (task_id,))
            row = cur.fetchone()
            conn.commit()
    if not row:
        return (None, None, None)

    # поддержка и dict_row, и tuple row
    if hasattr(row, "get"):
        return (row.get("status"), row.get("error"), row.get("backend_job_id"))
    return (row[0], row[1], row[2])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["real", "demo"], default="real")
    p.add_argument("--idle-exit-seconds", type=int, default=10)
    p.add_argument("--poll-seconds", type=float, default=1.0)
    p.add_argument("--job-poll-seconds", type=float, default=2.0)

    # сколько ждать callback после того как squeue перестал видеть job
    p.add_argument("--finished-grace-seconds", type=int, default=20)

    args = p.parse_args()

    print(f"[slurm-orch] leased_by={LEASED_BY} mode={args.mode} target_backend=slurm")

    idle_start = None

    while True:
        task = lease_one_task(LEASED_BY, lease_seconds=LEASE_SECONDS, target_backend="slurm")

        if not task:
            if args.mode == "demo":
                if idle_start is None:
                    idle_start = time.time()
                elif (time.time() - idle_start) >= args.idle_exit_seconds:
                    print(f"[slurm-orch] idle for {args.idle_exit_seconds}s -> exit (demo mode)")
                    return
            time.sleep(args.poll_seconds)
            continue

        idle_start = None

        try:
            # Пока MVP: в Slurm запускаем только demo_sleep
            if task.task_type != "demo_sleep":
                raise NotImplementedError(
                    f"Slurm backend supports only demo_sleep right now, got {task.task_type}"
                )

            sleep_s = int(task.payload.get("sleep_s", 1))

            # ВАЖНО: передаём leased_by внутрь job, чтобы callback на bastion смог сделать mark_done()
            job = submit_demo_sleep(
                task_id=task.id,
                leased_by=LEASED_BY,
                sleep_s=sleep_s,
                payload=task.payload,
            )

            # отмечаем старт и job id
            mark_running(task.id, LEASED_BY, backend="slurm", backend_job_id=str(job.job_id))
            heartbeat(
                task.id,
                LEASED_BY,
                lease_seconds=LEASE_SECONDS,
                meta={
                    "stage": "submitted",
                    "slurm_job_id": str(job.job_id),
                },
            )

            finished_seen_at = None  # когда впервые увидели FINISHED

            # ждём завершения через БД (callback), а squeue используем как страховку
            while True:
                db_status, db_error, db_job_id = _get_task_status(task.id)

                # Если callback уже обновил БД — мы заканчиваем этот task
                if db_status in ("done", "failed", "canceled"):
                    print(f"[slurm-orch] task={task.id} finished via DB status={db_status} job={db_job_id}")
                    break

                # Если задача внезапно вернулась в queued (например, кто-то reset'нул) — просто выходим из ожидания
                if db_status == "queued":
                    print(f"[slurm-orch] task={task.id} returned to queued -> stop waiting")
                    break

                # heartbeat чтобы lease не протух
                state, _ = get_job_state(str(job.job_id))
                heartbeat(
                    task.id,
                    LEASED_BY,
                    lease_seconds=LEASE_SECONDS,
                    meta={"stage": "waiting", "squeue_state": state},
                )

                # Если job уже исчез из squeue (FINISHED), но callback не пришёл — даём небольшой grace
                if state == "FINISHED":
                    if finished_seen_at is None:
                        finished_seen_at = time.time()
                    elif (time.time() - finished_seen_at) >= args.finished_grace_seconds:
                        err = (
                            "Slurm job finished (not in squeue), but no callback updated DB.\n"
                            "Most likely: RESULT_BASE_URL/RESULT_SECRET not exported into job, "
                            "or network/SG blocks HTTP, or API not running.\n"
                            f"job_id={job.job_id}"
                        )
                        mark_failed(task.id, LEASED_BY, error=err, retry=False)
                        print(f"[slurm-orch] failed task={task.id} job={job.job_id} reason=no_callback")
                        break
                else:
                    finished_seen_at = None

                time.sleep(args.job_poll_seconds)

        except Exception as e:
            err = f"{e}\n{traceback.format_exc()}"
            retry = (task.attempts < task.max_attempts)
            mark_failed(task.id, LEASED_BY, error=err, retry=retry)
            print(f"[slurm-orch] failed task={task.id} retry={retry}")


if __name__ == "__main__":
    main()