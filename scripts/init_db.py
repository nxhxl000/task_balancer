import os
import json
import uuid
import random

from dotenv import load_dotenv
import psycopg

load_dotenv()

DSN = os.environ.get("DATABASE_URL")
if not DSN:
    raise RuntimeError("DATABASE_URL is not set. Create .env and set DATABASE_URL=...")

DDL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'task_status') THEN
        CREATE TYPE task_status AS ENUM ('queued','leased','running','done','failed','canceled');
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS tasks (
    id               UUID PRIMARY KEY,
    task_type        TEXT NOT NULL,                         -- latin_square_from_prefix | mols_search
    status           task_status NOT NULL DEFAULT 'queued',

    n                INT NOT NULL CHECK (n > 0),
    priority         INT NOT NULL DEFAULT 100,
    attempts         INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 10,

    leased_by        TEXT NULL,
    lease_expires_at TIMESTAMPTZ NULL,

    payload          JSONB NOT NULL,
    result           JSONB NULL,
    error            TEXT NULL,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tasks_queue
ON tasks (status, priority, created_at);

CREATE INDEX IF NOT EXISTS idx_tasks_lease
ON tasks (status, lease_expires_at);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_tasks_updated ON tasks;
CREATE TRIGGER trg_tasks_updated
BEFORE UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
"""

def make_prefix_matrix(n: int, filled_rows: int = 1):
    """prefix: n×n matrix, filled cells are 0..n-1, empty are null"""
    m = [[None for _ in range(n)] for _ in range(n)]
    for c in range(n):
        m[0][c] = c
    if filled_rows >= 2 and n >= 2:
        for c in range(n):
            m[1][c] = (c + 1) % n
    return m

def seed_tasks(conn, count_ls: int = 5, count_mols: int = 5):
    rows = []

    # TaskType: latin_square_from_prefix
    for _ in range(count_ls):
        n = random.choice([3, 4, 5, 6, 7])
        prefix = make_prefix_matrix(n, filled_rows=random.choice([1, 2]))
        payload = {
            "problem": "complete_latin_square_from_prefix",
            "prefix_format": "matrix_nulls",
            "prefix": prefix,
            "constraints": {"latin": True, "symmetry_breaking": {"fix_first_row": True}},
            "output": {"return_one_solution": True},
        }
        rows.append((str(uuid.uuid4()), "latin_square_from_prefix", n, 50, json.dumps(payload)))

    # TaskType: mols_search (Jacobson–Matthews)
    for _ in range(count_mols):
        n = random.choice([3, 4, 5, 7, 8, 9])
        payload = {
            "problem": "search_mols",
            "method": "Jacobson-Matthews",
            "n": n,
            "k": 2,
            "seed": random.randint(1, 2**31 - 1),
            "budget": {"max_steps": 2_000_000, "time_limit_sec": 600},
        }
        rows.append((str(uuid.uuid4()), "mols_search", n, 100, json.dumps(payload)))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO tasks (id, task_type, n, priority, payload)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (id) DO NOTHING;
            """,
            rows,
        )

def main():
    with psycopg.connect(DSN) as conn:
        conn.execute(DDL)
        seed_tasks(conn, count_ls=6, count_mols=6)
    print("✅ DB initialized: tasks table created + seed inserted.")

if __name__ == "__main__":
    main()