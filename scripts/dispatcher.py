import os
import socket
import uuid
import psycopg

DSN = os.environ["DATABASE_URL"]

LEASE_SECONDS = 120  # 2 минуты, потом можно продлевать heartbeat'ом
LEASED_BY = f"{socket.gethostname()}:{uuid.uuid4()}"

LEASE_SQL = """
WITH candidate AS (
  SELECT id
  FROM tasks
  WHERE
    status = 'queued'
    OR (status = 'leased' AND lease_expires_at < now())
  ORDER BY priority DESC, created_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE tasks t
SET
  status = 'leased',
  leased_by = %s,
  leased_at = now(),
  lease_expires_at = now() + (%s::int || ' seconds')::interval,
  attempts = CASE WHEN t.status = 'queued' THEN t.attempts + 1 ELSE t.attempts END
FROM candidate
WHERE t.id = candidate.id
RETURNING t.id, t.task_type, t.payload, t.attempts, t.max_attempts;
"""

def lease_one_task(conn):
    # Важно: транзакция. В psycopg она автоматически открывается в контексте `with conn:`
    with conn:
        with conn.cursor() as cur:
            cur.execute(LEASE_SQL, (LEASED_BY, LEASE_SECONDS))
            row = cur.fetchone()
            return row  # None если задач нет

def main():
    with psycopg.connect(DSN) as conn:
        task = lease_one_task(conn)
        if not task:
            print("No tasks available")
            return
        print("Leased:", task)

if __name__ == "__main__":
    main()
