import json
import uuid
from app.core.db import get_conn

def main():
    run_id = str(uuid.uuid4())
    with get_conn() as conn:
        with conn.cursor() as cur:
            for i in range(10):
                payload = {"sleep_s": 2, "i": i}
                cur.execute(
                    """
                    INSERT INTO tasks (id, task_type, status, n, priority, attempts, max_attempts, payload, run_id, target_backend)
                    VALUES (%s::uuid, %s, 'queued', %s, %s, 0, 10, %s::jsonb, %s::uuid, %s)
                    """,
                    (str(uuid.uuid4()), "demo_sleep", 1, 100, json.dumps(payload), run_id, "local"),
                )
        conn.commit()
    print("enqueued run_id=", run_id)

if __name__ == "__main__":
    main()
