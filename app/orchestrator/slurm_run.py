import argparse
import socket
import uuid
import time
import traceback

from app.core.queue import lease_one_task, heartbeat, mark_running, mark_done, mark_failed
from app.backend.slurm.client import submit_demo_sleep, get_job_state, read_json_file, read_text_file

LEASE_SECONDS = 120
LEASED_BY = f"{socket.gethostname()}:{uuid.uuid4()}"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["real", "demo"], default="real")
    p.add_argument("--idle-exit-seconds", type=int, default=10)
    p.add_argument("--poll-seconds", type=float, default=1.0)
    p.add_argument("--job-poll-seconds", type=float, default=2.0)
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
                raise NotImplementedError(f"Slurm backend supports only demo_sleep right now, got {task.task_type}")

            sleep_s = int(task.payload.get("sleep_s", 1))
            job = submit_demo_sleep(task_id=task.id, sleep_s=sleep_s, payload=task.payload)

            # отмечаем старт и job id
            mark_running(task.id, LEASED_BY, backend="slurm", backend_job_id=job.job_id)
            heartbeat(task.id, LEASED_BY, lease_seconds=LEASE_SECONDS, meta={
                "stage": "submitted",
                "slurm_job_id": job.job_id,
                "workdir": job.workdir,
                "stdout": job.stdout_path,
                "stderr": job.stderr_path,
                "result": job.result_path,
            })

            # ждём завершения
            while True:
                state, exit_code = get_job_state(job.job_id)

                heartbeat(task.id, LEASED_BY, lease_seconds=LEASE_SECONDS, meta={"stage": "waiting", "state": state})

                if state.startswith("COMPLETED"):
                    result = read_json_file(job.result_path)
                    mark_done(task.id, LEASED_BY, result=result)
                    print(f"[slurm-orch] done task={task.id} job={job.job_id}")
                    break

                if state.startswith(("FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY")):
                    stderr = read_text_file(job.stderr_path)
                    stdout = read_text_file(job.stdout_path)
                    err = f"Slurm job failed state={state} exit={exit_code}\nSTDERR:\n{stderr}\nSTDOUT:\n{stdout}"
                    mark_failed(task.id, LEASED_BY, error=err, retry=False)
                    print(f"[slurm-orch] failed task={task.id} job={job.job_id} state={state}")
                    break

                time.sleep(args.job_poll_seconds)

        except Exception as e:
            err = f"{e}\n{traceback.format_exc()}"
            retry = (task.attempts < task.max_attempts)
            mark_failed(task.id, LEASED_BY, error=err, retry=retry)
            print(f"[slurm-orch] failed task={task.id} retry={retry}")


if __name__ == "__main__":
    main()
