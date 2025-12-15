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


def submit_demo_sleep(task_id: str, sleep_s: int, payload: dict[str, Any]) -> SlurmJob:
    """
    MVP submit:
    - создает workdir (желательно на общем FS, либо на bastion если воркеры видят этот путь)
    - sbatch job:
        * sleep
        * пишет result.json (успех) либо error.txt (ошибка) в workdir
    """
    base_dir = os.environ.get("SLURM_TASK_DIR", "/tmp/task_balancer")
    workdir = os.path.join(base_dir, task_id)
    os.makedirs(workdir, exist_ok=True)

    stdout_path = os.path.join(workdir, "stdout.txt")
    stderr_path = os.path.join(workdir, "stderr.txt")
    result_path = os.path.join(workdir, "result.json")
    error_path = os.path.join(workdir, "error.txt")

    # ВАЖНО: payload передаём как JSON-строку и экранируем для bash
    payload_json = json.dumps(payload, ensure_ascii=False)
    payload_q = shlex.quote(payload_json)  # безопасно для bash

    # bash-скрипт для --wrap (ТОЛЬКО строка bash, без "bash -lc ...")
    # Делает:
    #  - sleep
    #  - пытается написать result.json
    #  - если что-то упало -> пишет error.txt и exit 1
    wrap = f"""
set -euo pipefail
cd {shlex.quote(workdir)}
sleep {int(sleep_s)}
python3 - <<'PY'
import json, sys, traceback
payload = json.loads({payload_q})
out = {{
  "ok": True,
  "task_type": "demo_sleep",
  "slept": {int(sleep_s)},
  "echo": payload
}}
open({json.dumps(result_path)}, "w", encoding="utf-8").write(json.dumps(out, ensure_ascii=False))
print("WROTE_RESULT")
PY
""".strip()

    # Если python упадёт — set -e завершит job, но error.txt не появится.
    # Поэтому делаем обёртку через bash, которая ловит ошибку и пишет error.txt:
    wrap = f"""
set -euo pipefail
cd {shlex.quote(workdir)}
(
  sleep {int(sleep_s)}
  python3 - <<'PY'
import json
payload = json.loads({payload_q})
out = {{
  "ok": True,
  "task_type": "demo_sleep",
  "slept": {int(sleep_s)},
  "echo": payload
}}
open({json.dumps(result_path)}, "w", encoding="utf-8").write(json.dumps(out, ensure_ascii=False))
print("WROTE_RESULT")
PY
) || (
  code=$?
  echo "demo_sleep failed with exit=$code" > {shlex.quote(error_path)}
  exit $code
)
""".strip()

    submit_cmd = [
        "sbatch",
        "--parsable",
        "--job-name", f"taskbal_{task_id[:8]}",
        "--output", stdout_path,
        "--error", stderr_path,
        "--wrap", wrap,
    ]

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
    - если job исчез из squeue -> возвращаем FINISHED (оркестратор читает result/error файлы)
    """
    try:
        # %T = state; -h без заголовка
        out = _run(["squeue", "-j", str(job_id), "-h", "-o", "%T"])
        state = out.strip()
        if not state:
            return ("FINISHED", None)
        # Например: RUNNING, PENDING, COMPLETING...
        return (state, None)
    except Exception:
        # если squeue вернул ошибку — считаем, что job уже не существует
        return ("FINISHED", None)


def read_json_file(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_text_file(path: str, max_chars: int = 8000) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read(max_chars)
    except FileNotFoundError:
        return ""
