import os
import json
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
import psycopg


def fetchall_dict(cur):
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def main1():
    load_dotenv()

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set. Create .env and set DATABASE_URL=...")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # 1) Список таблиц (обычно всё в схеме public)
            cur.execute(
                """
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, tablename;
                """
            )
            tables = cur.fetchall()

            if not tables:
                print("No user tables found.")
                return

            for schema, table in tables:
                print("\n" + "=" * 80)
                print(f"TABLE: {schema}.{table}")
                print("=" * 80)

                cur.execute(f'SELECT * FROM "{schema}"."{table}" ORDER BY 1;')
                rows = fetchall_dict(cur)

                print(f"Rows: {len(rows)}")
                print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))

            # 2) (опционально) Показать enum-типы (например task_status)
            print("\n" + "=" * 80)
            print("ENUM TYPES")
            print("=" * 80)
            cur.execute(
                """
                SELECT n.nspname AS schema, t.typname AS enum_name,
                       array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values
                FROM pg_type t
                JOIN pg_enum e ON t.oid = e.enumtypid
                JOIN pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                GROUP BY n.nspname, t.typname
                ORDER BY n.nspname, t.typname;
                """
            )
            enums = fetchall_dict(cur)
            print(json.dumps(enums, ensure_ascii=False, indent=2, default=str))

def main():
    load_dotenv()

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set. Create .env and set DATABASE_URL=...")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM public.tasks ORDER BY created_at DESC;')
            rows = fetchall_dict(cur)

    # имя файла с датой/временем
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(f"tasks_dump_{ts}.json")

    # сохраняем
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)

    print(f"✅ Saved {len(rows)} rows to: {out_path.resolve()}")

if __name__ == "__main__":
    main()
