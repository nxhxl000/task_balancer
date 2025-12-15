import argparse
import socket
import uuid
import time
import traceback

from app.core.queue import lease_one_task, heartbeat, mark_running, mark_done, mark_failed
from app.core.worker_local import execute_local

LEASE_SECONDS = 120
DEFAULT_POLL_SECONDS = 1.0

LEASED_BY = f"{socket.gethostname()}:{uuid.uuid4()}"


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--mode",
        choices=["real", "demo"],
        default="real",
        help="real = run forever, demo = exit after idle period",
    )
    p.add_argument(
        "--idle-exit-seconds",
        type=int,
        default=10,
        help="Only for demo mode: exit if no tasks for S seconds (default 10)",
    )
    p.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="How often to poll DB when no tasks",
    )
    args = p.parse_args()

    print(f"[orchestrator] leased_by={LEASED_BY} mode={args.mode} target_backend=local")

    idle_start = None  # когда началась полоса "нет задач"

    while True:
        task = lease_one_task(LEASED_BY, lease_seconds=LEASE_SECONDS, target_backend="local")

        if not task:
            if args.mode == "demo":
                if idle_start is None:
                    idle_start = time.time()
                elif (time.time() - idle_start) >= args.idle_exit_seconds:
                    print(f"[orchestrator] idle for {args.idle_exit_seconds}s -> exit (demo mode)")
                    return

            time.sleep(args.poll_seconds)
            continue

        # задача нашлась — сбрасываем idle
        idle_start = None

        try:
            mark_running(task.id, LEASED_BY, backend="local", backend_job_id="")
            heartbeat(task.id, LEASED_BY, lease_seconds=LEASE_SECONDS, meta={"stage": "executing"})

            result = execute_local(task.task_type, task.payload)

            mark_done(task.id, LEASED_BY, result=result)
            print(f"[orchestrator] done task={task.id} type={task.task_type}")

        except Exception as e:
            err = f"{e}\n{traceback.format_exc()}"
            retry = (task.attempts < task.max_attempts)
            mark_failed(task.id, LEASED_BY, error=err, retry=retry)
            print(f"[orchestrator] failed task={task.id} retry={retry}")


if __name__ == "__main__":
    main()
