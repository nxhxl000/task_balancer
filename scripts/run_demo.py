import argparse
import json
import time
import uuid
from app.core.db import get_conn


TERMINAL_STATUSES = ("done", "failed", "canceled")


def enqueue_demo(run_id: str, n_tasks: int, sleep_s: int, priority: int = 100) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            for i in range(n_tasks):
                payload = {"sleep_s": sleep_s, "i": i}
                cur.execute(
                    """
                    INSERT INTO tasks (id, task_type, status, n, priority, attempts, max_attempts, payload, run_id, target_backend)
                    VALUES (%s::uuid, %s, 'queued', %s, %s, 0, 10, %s::jsonb, %s::uuid, %s)
                    """,
                    (str(uuid.uuid4()), "demo_sleep", 1, priority, json.dumps(payload), run_id, "local"),
                )
        conn.commit()


def delete_run(run_id: str) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS cnt FROM tasks WHERE run_id = %s::uuid;", (run_id,))
            cnt = cur.fetchone()["cnt"]
            cur.execute("DELETE FROM tasks WHERE run_id = %s::uuid;", (run_id,))
        conn.commit()
    return cnt


def get_run_stats(run_id: str) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  count(*) AS total,
                  count(*) FILTER (WHERE status = 'queued') AS queued,
                  count(*) FILTER (WHERE status = 'leased') AS leased,
                  count(*) FILTER (WHERE status = 'running') AS running,
                  count(*) FILTER (WHERE status = 'done') AS done,
                  count(*) FILTER (WHERE status = 'failed') AS failed,
                  count(*) FILTER (WHERE status = 'canceled') AS canceled,
                  max(attempts) AS max_attempts_seen
                FROM tasks
                WHERE run_id = %s::uuid;
                """,
                (run_id,),
            )
            return cur.fetchone()


def is_finished(stats: dict) -> bool:
    return stats["total"] == (stats["done"] + stats["failed"] + stats["canceled"])


def main():
    p = argparse.ArgumentParser(description="Create and monitor a demo run")
    p.add_argument("--tasks", type=int, default=10, help="number of tasks to enqueue")
    p.add_argument("--sleep", type=int, default=2, help="sleep seconds per task")
    p.add_argument("--priority", type=int, default=100, help="task priority")
    p.add_argument("--poll", type=float, default=1.0, help="poll interval seconds")
    p.add_argument("--timeout", type=int, default=300, help="timeout seconds")
    p.add_argument("--cleanup-run-id", default=None, help="run_id to delete before starting (optional)")
    p.add_argument("--yes", action="store_true", help="confirm deletion if cleanup-run-id is set")
    args = p.parse_args()

    if args.cleanup_run_id:
        if not args.yes:
            print("[run_demo] cleanup requested but --yes not provided. Dry-run only.")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) AS cnt FROM tasks WHERE run_id = %s::uuid;", (args.cleanup_run_id,))
                    cnt = cur.fetchone()["cnt"]
                    print(f"[run_demo] would delete {cnt} tasks for run_id={args.cleanup_run_id}")
            return

        deleted = delete_run(args.cleanup_run_id)
        print(f"[run_demo] deleted {deleted} tasks for run_id={args.cleanup_run_id}")

    run_id = str(uuid.uuid4())
    enqueue_demo(run_id, n_tasks=args.tasks, sleep_s=args.sleep, priority=args.priority)
    print(f"[run_demo] enqueued run_id={run_id} tasks={args.tasks} sleep={args.sleep}s")
    print("[run_demo] start orchestrator in another terminal if not running:")
    print("          python -m app.orchestrator.run")

    start = time.time()
    last_print = 0.0

    while True:
        stats = get_run_stats(run_id)

        now = time.time()
        if now - last_print >= 1.0:
            print(
                f"[run_demo] total={stats['total']} "
                f"queued={stats['queued']} leased={stats['leased']} running={stats['running']} "
                f"done={stats['done']} failed={stats['failed']} canceled={stats['canceled']} "
                f"max_attempts_seen={stats['max_attempts_seen']}"
            )
            last_print = now

        if is_finished(stats):
            print("[run_demo] finished ✅")
            break

        if now - start > args.timeout:
            print("[run_demo] timeout ⏰ (some tasks not finished)")
            break

        time.sleep(args.poll)

    # финальная сводка
    final = get_run_stats(run_id)
    print("[run_demo] final:", final)


if __name__ == "__main__":
    main()
