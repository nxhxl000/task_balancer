from __future__ import annotations

import json
import os
import shlex
import subprocess
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class SlurmJob:
    job_id: str
    workdir: str
    stdout_path: str
    stderr_path: str
    result_path: str
    error_path: str


def _run(cmd: list[str]) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {cmd}\nstdout={p.stdout}\nstderr={p.stderr}")
    return p.stdout.strip()


def submit_demo_sleep(
    task_id: str,
    leased_by: str,
    sleep_s: int,
    payload: dict[str, Any],
    nodelist: Optional[str] = None,
) -> SlurmJob:
    """
    Submit Slurm job (без shared FS):
      - job выполняет sleep / вычисления
      - отправляет результат на bastion API: POST {RESULT_BASE_URL}/v1/task-result
      - подпись: header x-task-sig = HMAC_SHA256(RESULT_SECRET, body_bytes).hexdigest()
      - bastion (FastAPI) делает mark_done/mark_failed в БД

    Требуемые env (должны быть доступны при sbatch, чтобы экспортнулись в job):
      - RESULT_BASE_URL (например http://10.128.0.14:8080)
      - RESULT_SECRET   (длинный секрет)
    """
    base_url = os.environ.get("RESULT_BASE_URL", "").strip()
    secret = os.environ.get("RESULT_SECRET", "").strip()

    if not base_url:
        raise RuntimeError("RESULT_BASE_URL is required (e.g. http://10.128.0.14:8080). Did you source .env?")
    if not secret:
        raise RuntimeError("RESULT_SECRET is required (must match bastion). Did you source .env?")

    # Пути ниже чисто для логов/дебага (они на worker'е, bastion их не читает)
    workdir = f"/tmp/task_balancer/{task_id}"
    stdout_path = f"/tmp/taskbal_{task_id[:8]}_%j.out"
    stderr_path = f"/tmp/taskbal_{task_id[:8]}_%j.err"
    result_path = f"{workdir}/result.json"  # не используется, оставлено для совместимости
    error_path = f"{workdir}/error.txt"     # не используется, оставлено для совместимости

    payload_json = json.dumps(payload, ensure_ascii=False)
    payload_q = shlex.quote(payload_json)  # безопасно для bash

    # Python внутри job: делает sleep, формирует payload, подписывает и POST'ит в bastion
    job_py = f"""\
import json, time, hmac, hashlib, urllib.request, traceback, os, socket

BASE = os.environ.get("RESULT_BASE_URL", {json.dumps(base_url)})
SECRET = os.environ.get("RESULT_SECRET", "").encode("utf-8")
if not SECRET:
    raise SystemExit("RESULT_SECRET is empty (not exported into job)")

task_id = {json.dumps(task_id)}
leased_by = {json.dumps(leased_by)}
sleep_s = int({int(sleep_s)})

payload = json.loads({payload_q})

# ✅ метаданные о том, где реально выполнялся job
slurm_job_id = os.environ.get("SLURM_JOB_ID", "")
slurm_nodelist = os.environ.get("SLURM_NODELIST", "")
node = os.environ.get("SLURMD_NODENAME") or socket.gethostname()

def post(data: dict) -> None:
    body = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    sig = hmac.new(SECRET, body, hashlib.sha256).hexdigest()

    last_err = None
    for _ in range(5):
        try:
            req = urllib.request.Request(
                BASE + "/v1/task-result",
                data=body,
                headers={{"content-type": "application/json", "x-task-sig": sig}},
                method="POST",
            )
            resp = urllib.request.urlopen(req, timeout=5).read().decode()
            print(resp)
            return
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise last_err

try:
    time.sleep(sleep_s)
    result = {{
        "ok": True,
        "task_type": "demo_sleep",
        "slept": sleep_s,
        "echo": payload,

        # ✅ добавили: на каком узле/какой job
        "node": node,
        "slurm_job_id": slurm_job_id,
        "slurm_nodelist": slurm_nodelist,
    }}
    post({{
        "task_id": task_id,
        "leased_by": leased_by,
        "ok": True,
        "result": result
    }})
    raise SystemExit(0)
except Exception as e:
    err = str(e) + "\\n" + traceback.format_exc()
    try:
        post({{
            "task_id": task_id,
            "leased_by": leased_by,
            "ok": False,
            "error": err
        }})
    except Exception as e2:
        print("FAILED_TO_POST_ERROR:", repr(e2))
        print(err)
    raise SystemExit(2)
"""

    # ВАЖНО: Slurm запускает --wrap через /bin/sh, поэтому set -o pipefail ломается.
    # Решение: явно запускаем bash -lc "<script>"
    script = f"""
set -euo pipefail
mkdir -p {shlex.quote(workdir)}
python3 - <<'PY'
{job_py}
PY
""".strip()

    wrap = f"bash -lc {shlex.quote(script)}"

    submit_cmd = [
        "sbatch",
        "--parsable",
        "--job-name", f"taskbal_{task_id[:8]}",
        "--output", stdout_path,
        "--error", stderr_path,
        "--export", "ALL,RESULT_BASE_URL,RESULT_SECRET",
        "--wrap", wrap,
    ]

    if nodelist:
        submit_cmd += ["--nodelist", nodelist]

    job_id = _run(submit_cmd)

    return SlurmJob(
        job_id=job_id,
        workdir=workdir,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        result_path=result_path,
        error_path=error_path,
    )


def get_job_state(job_id: str) -> tuple[str, Optional[int]]:
    """
    Без sacct:
      - пока job виден в squeue -> возвращаем его состояние (RUNNING/PENDING/...)
      - если job исчез из squeue -> FINISHED
    """
    p = subprocess.run(
        ["squeue", "-j", str(job_id), "-h", "-o", "%T"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p.returncode != 0:
        return ("FINISHED", None)

    state = (p.stdout or "").strip()
    if not state:
        return ("FINISHED", None)

    return (state, None)


def read_json_file(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_text_file(path: str, max_chars: int = 8000) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read(max_chars)
    except FileNotFoundError:
        return ""
