import argparse
import socket
import uuid
import time
import traceback
from typing import Optional, Tuple, Any
import json
import os

from dotenv import load_dotenv

from app.core.queue import lease_one_task, heartbeat, mark_running, mark_failed, mark_done
from app.core.db import get_conn

LEASE_SECONDS = 120
LEASED_BY = f"{socket.gethostname()}:{uuid.uuid4()}"

TASK_STATUS_SQL = """
SELECT status, error, backend_job_id
FROM tasks
WHERE id = %s::uuid
"""

def _get_task_status(task_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Возвращает (status, error, backend_job_id) из БД."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(TASK_STATUS_SQL, (task_id,))
            row = cur.fetchone()
            conn.commit()
    if not row:
        return (None, None, None)

    # поддержка и dict_row, и tuple row
    if hasattr(row, "get"):
        return (row.get("status"), row.get("error"), row.get("backend_job_id"))
    return (row[0], row[1], row[2])


def _normalize_payload(payload: Any) -> dict:
    """
    В Neon jsonb обычно приходит dict.
    Но если по какой-то причине пришло как str/bytes/пустая строка — приводим к dict,
    чтобы не падать на payload.get().
    """
    if payload is None:
        return {}

    if isinstance(payload, dict):
        return payload

    if isinstance(payload, (bytes, bytearray, memoryview)):
        payload = bytes(payload).decode("utf-8", "replace")

    if isinstance(payload, str):
        s = payload.strip()
        if not s:
            return {}
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            # на случай формата вида {""i"":6,...}
            if '""' in s:
                return json.loads(s.replace('""', '"'))
            raise

    # последний шанс: завернём во что-то осмысленное
    return {"_raw": str(payload)}


def main():
    load_dotenv()

    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["real", "demo"], default="demo")
    p.add_argument("--idle-exit-seconds", type=int, default=10)
    p.add_argument("--poll-seconds", type=float, default=1.0)
    p.add_argument("--work-poll-seconds", type=float, default=0.5)

    # чтобы демо не смешивалось с реальными: обрабатываем только task_type с префиксом
    p.add_argument("--demo-prefix", type=str, default="boinc_demo_")

    args = p.parse_args()

    if not os.environ.get("DATABASE_URL"):
        raise SystemExit("[boinc-orch] missing env DATABASE_URL. Проверь .env (Neon).")

    print(f"[boinc-orch] leased_by={LEASED_BY} mode={args.mode} target_backend=boinc demo_prefix={args.demo_prefix}")

    idle_start = None

    while True:
        task = lease_one_task(LEASED_BY, lease_seconds=LEASE_SECONDS, target_backend="boinc")

        if not task:
            if args.mode == "demo":
                if idle_start is None:
                    idle_start = time.time()
                elif (time.time() - idle_start) >= args.idle_exit_seconds:
                    print(f"[boinc-orch] idle for {args.idle_exit_seconds}s -> exit (demo mode)")
                    return
            time.sleep(args.poll_seconds)
            continue

        idle_start = None

        try:
            # нормализуем payload, чтобы не падать на строках/пустых значениях
            task.payload = _normalize_payload(task.payload)

            # защита: в demo-режиме не трогаем реальные boinc-задачи
            if args.mode == "demo" and not task.task_type.startswith(args.demo_prefix):
                err = f"Not a demo task: task_type={task.task_type} (expected prefix '{args.demo_prefix}')"
                # вернём в очередь, чтобы случайно не съесть
                mark_failed(task.id, LEASED_BY, error=err, retry=True)
                print(f"[boinc-orch] skip non-demo task={task.id} type={task.task_type} -> returned to queued")
                continue

            if task.task_type != "boinc_demo_sleep":
                raise NotImplementedError(
                    f"BOINC demo orchestrator supports only boinc_demo_sleep right now, got {task.task_type}"
                )

            sleep_s = int(task.payload.get("sleep_s", 1))
            backend_job_id = f"dryrun_{task.id.replace('-', '')}"

            mark_running(task.id, LEASED_BY, backend="boinc", backend_job_id=backend_job_id)
            heartbeat(
                task.id,
                LEASED_BY,
                lease_seconds=LEASE_SECONDS,
                meta={"stage": "running_dryrun", "sleep_s": sleep_s, "backend_job_id": backend_job_id},
            )

            t0 = time.time()
            while (time.time() - t0) < sleep_s:
                heartbeat(
                    task.id,
                    LEASED_BY,
                    lease_seconds=LEASE_SECONDS,
                    meta={"stage": "sleeping", "elapsed_s": round(time.time() - t0, 2), "sleep_s": sleep_s},
                )
                time.sleep(args.work_poll_seconds)

            result = {
                "ok": True,
                "dryrun": True,
                "task_type": task.task_type,
                "payload_echo": task.payload,
                "meta": {"backend_job_id": backend_job_id},
            }
            mark_done(task.id, LEASED_BY, result)

            db_status, db_error, db_job_id = _get_task_status(task.id)
            if db_status == "failed" and db_error:
                print(f"[boinc-orch] task={task.id} finished via DB status=failed job={db_job_id}\nerror:\n{db_error[:2000]}")
            else:
                print(f"[boinc-orch] task={task.id} finished via DB status={db_status} job={db_job_id}")

        except Exception as e:
            err = f"{e}\n{traceback.format_exc()}"
            retry = (task.attempts < task.max_attempts)
            mark_failed(task.id, LEASED_BY, error=err, retry=retry)
            print(f"[boinc-orch] failed task={task.id} retry={retry}")


if __name__ == "__main__":
    main()
