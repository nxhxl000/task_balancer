import argparse
from app.core.db import get_conn


REQUEUE_LEASED_SQL = """
UPDATE tasks
SET
  status = 'queued',
  leased_by = NULL,
  leased_at = NULL,
  lease_expires_at = NULL,
  last_heartbeat_at = NULL
WHERE
  status = 'leased'
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at < now()
RETURNING id::text;
"""

REQUEUE_RUNNING_SQL = """
UPDATE tasks
SET
  status = 'queued',
  leased_by = NULL,
  leased_at = NULL,
  lease_expires_at = NULL,
  last_heartbeat_at = NULL,
  backend = NULL,
  backend_job_id = NULL,
  started_at = NULL
WHERE
  status = 'running'
  AND last_heartbeat_at IS NOT NULL
  AND last_heartbeat_at < now() - (%s::int || ' seconds')::interval
RETURNING id::text;
"""


def main():
    p = argparse.ArgumentParser(description="Requeue stale leased/running tasks")
    p.add_argument("--running-stale-seconds", type=int, default=600,
                   help="if running heartbeat older than this -> requeue (default 600s)")
    p.add_argument("--yes", action="store_true", help="actually update (otherwise dry-run)")
    args = p.parse_args()

    with get_conn() as conn:
        with conn.cursor() as cur:
            # counts
            cur.execute("""
                SELECT
                  (SELECT count(*) FROM tasks WHERE status='leased' AND lease_expires_at < now()) AS leased_stale,
                  (SELECT count(*) FROM tasks WHERE status='running' AND last_heartbeat_at < now() - (%s::int || ' seconds')::interval) AS running_stale
            """, (args.running_stale_seconds,))
            row = cur.fetchone()
            print(f"[requeue_stale] stale leased: {row['leased_stale']}, stale running: {row['running_stale']}")

            if not args.yes:
                print("[requeue_stale] dry-run only. Add --yes to apply.")
                return

            # apply
            cur.execute(REQUEUE_LEASED_SQL)
            leased_ids = [r["id"] for r in cur.fetchall()]

            cur.execute(REQUEUE_RUNNING_SQL, (args.running_stale_seconds,))
            running_ids = [r["id"] for r in cur.fetchall()]

            conn.commit()
            print(f"[requeue_stale] requeued leased: {len(leased_ids)}")
            print(f"[requeue_stale] requeued running: {len(running_ids)}")


if __name__ == "__main__":
    main()
