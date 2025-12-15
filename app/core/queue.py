from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from .db import get_conn


@dataclass
class Task:
    id: str
    task_type: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    n: int
    priority: int
    status: str
    target_backend: Optional[str] = None
    backend: Optional[str] = None
    backend_job_id: Optional[str] = None


LEASE_SQL = """
WITH candidate AS (
  SELECT id
  FROM tasks
  WHERE
    (status = 'queued' OR (status = 'leased' AND lease_expires_at < now()))
    AND attempts < max_attempts
    AND status <> 'canceled'
    AND (
      (%s::text IS NOT NULL AND target_backend = %s::text)
      OR
      (%s::text IS NULL AND target_backend IS NULL)
    )
  ORDER BY priority DESC, created_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE tasks t
SET
  status = 'leased',
  leased_by = %s,
  leased_at = now(),
  last_heartbeat_at = now(),
  lease_expires_at = now() + (%s::int || ' seconds')::interval,
  attempts = CASE WHEN t.status = 'queued' THEN t.attempts + 1 ELSE t.attempts END
FROM candidate
WHERE t.id = candidate.id
RETURNING
  t.id::text,
  t.task_type,
  t.payload,
  t.attempts,
  t.max_attempts,
  t.n,
  t.priority,
  t.status,
  t.target_backend,
  t.backend,
  t.backend_job_id;
"""


def lease_one_task(
    leased_by: str,
    lease_seconds: int = 120,
    target_backend: Optional[str] = "local",
) -> Optional[Task]:
    """
    target_backend:
      - "local"/"slurm"/"boinc": брать только задачи с таким target_backend
      - None: брать только задачи, где target_backend IS NULL
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                LEASE_SQL,
                (
                    target_backend,
                    target_backend,
                    target_backend,
                    leased_by,
                    lease_seconds,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            if not row:
                return None
            return Task(
                id=row["id"],
                task_type=row["task_type"],
                payload=row["payload"],
                attempts=row["attempts"],
                max_attempts=row["max_attempts"],
                n=row["n"],
                priority=row["priority"],
                status=row["status"],
                target_backend=row.get("target_backend"),
                backend=row.get("backend"),
                backend_job_id=row.get("backend_job_id"),
            )


HEARTBEAT_SQL = """
UPDATE tasks
SET
  lease_expires_at = now() + (%s::int || ' seconds')::interval,
  last_heartbeat_at = now(),
  worker_meta = COALESCE(worker_meta, '{}'::jsonb) || %s::jsonb
WHERE
  id = %s::uuid
  AND leased_by = %s
  AND status IN ('leased', 'running');
"""


def heartbeat(task_id: str, leased_by: str, lease_seconds: int = 120, meta: Optional[dict[str, Any]] = None) -> None:
    meta_json = json.dumps(meta or {})
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(HEARTBEAT_SQL, (lease_seconds, meta_json, task_id, leased_by))
            conn.commit()


MARK_RUNNING_SQL = """
UPDATE tasks
SET
  status = 'running',
  backend = %s,
  backend_job_id = %s,
  started_at = COALESCE(started_at, now()),
  last_heartbeat_at = now()
WHERE id = %s::uuid AND leased_by = %s AND status = 'leased';
"""


def mark_running(task_id: str, leased_by: str, backend: str, backend_job_id: str = "") -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(MARK_RUNNING_SQL, (backend, backend_job_id, task_id, leased_by))
            conn.commit()


# ✅ enum содержит 'done', а не 'succeeded'
MARK_DONE_SQL = """
UPDATE tasks
SET
  status = 'done',
  result = %s::jsonb,
  error = NULL,
  finished_at = now(),
  exit_code = 0,
  lease_expires_at = NULL
WHERE id = %s::uuid AND leased_by = %s;
"""


def mark_done(task_id: str, leased_by: str, result: dict[str, Any]) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(MARK_DONE_SQL, (json.dumps(result), task_id, leased_by))
            conn.commit()


MARK_FAILED_SQL = """
UPDATE tasks
SET
  status = %s,
  error = %s,
  finished_at = CASE WHEN %s = 'failed' THEN now() ELSE finished_at END,
  exit_code = CASE WHEN %s = 'failed' THEN 1 ELSE exit_code END,
  leased_by = CASE WHEN %s = 'queued' THEN NULL ELSE leased_by END,
  lease_expires_at = CASE WHEN %s = 'queued' THEN NULL ELSE lease_expires_at END
WHERE id = %s::uuid
  AND leased_by = %s
  AND status <> 'canceled';
"""


def mark_failed(task_id: str, leased_by: str, error: str, retry: bool) -> None:
    # retry=True -> возвращаем в queued (пусть другой воркер возьмёт)
    new_status = "queued" if retry else "failed"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                MARK_FAILED_SQL,
                (new_status, error, new_status, new_status, new_status, new_status, task_id, leased_by),
            )
            conn.commit()
