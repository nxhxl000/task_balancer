import argparse
import socket
import uuid
import time
import traceback
from typing import Optional, Tuple
import os

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

    # ✅ round-robin по узлам (фиксируем узел через sbatch --nodelist)
    p.add_argument("--rr-nodes", type=str, default="",
                   help="Comma-separated Slurm node list for round-robin, e.g. worker1,worker2. If empty -> no RR.")

    args = p.parse_args()

    for k in ("RESULT_BASE_URL", "RESULT_SECRET"):
        if not os.environ.get(k):
            raise SystemExit(f"[slurm-orch] missing env {k}. Did you 'source .env' before запуск?")

    rr_nodes = [x.strip() for x in args.rr_nodes.split(",") if x.strip()]
    rr_i = 0  # локальный указатель RR (если запустишь 1 orchestrator — норм)

    print(f"[slurm-orch] leased_by={LEASED_BY} mode={args.mode} target_backend=slurm rr_nodes={rr_nodes or '-'}")

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
            SUPPORTED = {
                "complete_latin_square_from_prefix",
                "search_mols",
            }
            if task.task_type not in SUPPORTED:
                raise NotImplementedError(f"... got {task.task_type}")

            sleep_s = int(task.payload.get("sleep_s", 1))

            # ✅ выбираем узел RR (если включён)
            nodelist = None
            if rr_nodes:
                nodelist = rr_nodes[rr_i % len(rr_nodes)]
                rr_i += 1

            # ВАЖНО: передаём leased_by внутрь job, чтобы callback на bastion смог сделать mark_done()
            job = submit_demo_sleep(
                task_id=task.id,
                leased_by=LEASED_BY,
                sleep_s=sleep_s,
                payload=task.payload,
                nodelist=nodelist,  # ✅ новое
            )

            mark_running(task.id, LEASED_BY, backend="slurm", backend_job_id=str(job.job_id))
            heartbeat(
                task.id,
                LEASED_BY,
                lease_seconds=LEASE_SECONDS,
                meta={
                    "stage": "submitted",
                    "slurm_job_id": str(job.job_id),
                    "rr_nodelist": nodelist,  # ✅ чтобы видеть куда “пинали”
                },
            )

            finished_seen_at = None

            while True:
                db_status, db_error, db_job_id = _get_task_status(task.id)

                if db_status in ("done", "failed", "canceled"):
                    if db_status == "failed" and db_error:
                        print(
                            f"[slurm-orch] task={task.id} finished via DB status=failed job={db_job_id}\n"
                            f"error:\n{db_error[:2000]}"
                        )
                    else:
                        print(f"[slurm-orch] task={task.id} finished via DB status={db_status} job={db_job_id}")
                    break

                if db_status == "queued":
                    print(f"[slurm-orch] task={task.id} returned to queued -> stop waiting")
                    break

                state, _ = get_job_state(str(job.job_id))
                heartbeat(
                    task.id,
                    LEASED_BY,
                    lease_seconds=LEASE_SECONDS,
                    meta={"stage": "waiting", "squeue_state": state},
                )

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
