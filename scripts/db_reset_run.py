import argparse
from app.core.db import get_conn


def main():
    p = argparse.ArgumentParser(description="Delete tasks by run_id (cleanup demo runs)")
    p.add_argument("--run-id", required=True, help="run_id UUID")
    p.add_argument("--yes", action="store_true", help="actually delete (otherwise dry-run)")
    args = p.parse_args()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS cnt FROM tasks WHERE run_id = %s::uuid;", (args.run_id,))
            cnt = cur.fetchone()["cnt"]
            print(f"[db_reset_run] tasks to delete: {cnt} (run_id={args.run_id})")

            if not args.yes:
                print("[db_reset_run] dry-run only. Add --yes to delete.")
                return

            cur.execute("DELETE FROM tasks WHERE run_id = %s::uuid;", (args.run_id,))
            conn.commit()
            print("[db_reset_run] deleted.")


if __name__ == "__main__":
    main()