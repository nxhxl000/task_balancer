import argparse
from app.core.db import get_conn

RESET_SQL_TEMPLATE = """
UPDATE tasks
SET
  status = 'queued',
  {attempts_clause}
  leased_by = NULL,
  leased_at = NULL,
  lease_expires_at = NULL,
  last_heartbeat_at = NULL,
  backend = NULL,
  backend_job_id = NULL,
  started_at = NULL,
  finished_at = NULL,
  result = NULL,
  error = NULL,
  exit_code = NULL,
  worker_meta = NULL
WHERE task_type = ANY(%s)
  AND status <> 'canceled'
  {backend_filter}
RETURNING id::text;
"""

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--types", nargs="+", default=["latin_square_from_prefix", "mols_search"])
    p.add_argument("--only-backend", default=None, help="e.g. local")
    p.add_argument("--keep-attempts", action="store_true")
    p.add_argument("--yes", action="store_true")
    args = p.parse_args()

    attempts_clause = "" if args.keep_attempts else "attempts = 0,\n  "
    backend_filter = ""
    params = [args.types]

    if args.only_backend:
        backend_filter = "AND backend = %s"
        params.append(args.only_backend)

    sql = RESET_SQL_TEMPLATE.format(
        attempts_clause=attempts_clause,
        backend_filter=("\n  " + backend_filter) if backend_filter else ""
    )

    with get_conn() as conn:
        with conn.cursor() as cur:
            # dry-run count
            cur.execute(
                f"SELECT count(*) AS cnt FROM tasks WHERE task_type = ANY(%s) AND status <> 'canceled' "
                + ("AND backend = %s" if args.only_backend else ""),
                tuple(params),
            )
            cnt = cur.fetchone()["cnt"]
            print(f"[reset_real_tasks] matched: {cnt}")

            if not args.yes:
                print("[reset_real_tasks] dry-run only. Add --yes to apply.")
                return

            cur.execute(sql, tuple(params))
            ids = cur.fetchall()
        conn.commit()

    print(f"[reset_real_tasks] reset: {len(ids)} tasks")

if __name__ == "__main__":
    main()
