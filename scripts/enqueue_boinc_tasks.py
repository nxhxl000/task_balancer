import json
import uuid
from app.core.db import get_conn

# ====== НАСТРОЙКИ ======
NUM_SLEEP = 10
NUM_PREFIX = 3
NUM_MOLS = 5

PRIO_SLEEP = 1000
PRIO_PREFIX = 1200
PRIO_MOLS = 1100

MAX_ATTEMPTS = 10

PREFIX_5 = [
    [0, 1, 2, 3, 4],
    [None, None, None, None, None],
    [None, None, None, None, None],
    [None, None, None, None, None],
    [None, None, None, None, None],
]

def enqueue(cur, *, task_type: str, n: int, priority: int, payload: dict, run_id: str, target_backend: str = "boinc"):
    cur.execute(
        """
        INSERT INTO tasks (
            id, task_type, status, n, priority,
            attempts, max_attempts, payload, run_id, target_backend
        )
        VALUES (%s::uuid, %s, 'queued', %s, %s, 0, %s, %s::jsonb, %s::uuid, %s)
        """,
        (
            str(uuid.uuid4()),
            task_type,
            n,
            priority,
            MAX_ATTEMPTS,
            json.dumps(payload, ensure_ascii=False),
            run_id,
            target_backend,
        ),
    )

def main():
    run_id = str(uuid.uuid4())

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) Демонстрационные задачи для BOINC (тип с префиксом boinc_)
            for i in range(NUM_SLEEP):
                payload = {"i": i, "sleep_s": 2}  # payload.problem не трогаем
                enqueue(
                    cur,
                    task_type="boinc_demo_sleep",
                    n=1,
                    priority=PRIO_SLEEP,
                    payload=payload,
                    run_id=run_id,
                )

            for _ in range(NUM_PREFIX):
                payload = {
                    "output": {"return_one_solution": True},
                    "prefix": PREFIX_5,
                    "problem": "complete_latin_square_from_prefix",
                    "constraints": {"latin": True, "symmetry_breaking": {"fix_first_row": True}},
                    "prefix_format": "matrix_nulls",
                }
                enqueue(
                    cur,
                    task_type="boinc_demo_latin_square_from_prefix",
                    n=5,
                    priority=PRIO_PREFIX,
                    payload=payload,
                    run_id=run_id,
                )

            base_seed = 45203843
            for t in range(NUM_MOLS):
                payload = {
                    "k": 2,
                    "n": 9,
                    "seed": base_seed + t,
                    "budget": {"max_steps": 2_000_000, "time_limit_sec": 600},
                    "method": "Jacobson-Matthews",
                    "problem": "search_mols",
                }
                enqueue(
                    cur,
                    task_type="boinc_demo_search_mols",
                    n=9,
                    priority=PRIO_MOLS,
                    payload=payload,
                    run_id=run_id,
                )

        conn.commit()

    print("enqueued BOINC DEMO run_id=", run_id)
    print(f"  boinc_demo_sleep={NUM_SLEEP}, boinc_demo_latin_square_from_prefix={NUM_PREFIX}, boinc_demo_search_mols={NUM_MOLS}")

if __name__ == "__main__":
    main()
