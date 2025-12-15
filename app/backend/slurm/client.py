from __future__ import annotations

import json
import os
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


def _run(cmd: list[str]) -> str:
    p = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.stdout.strip()


def submit_demo_sleep(task_id: str, sleep_s: int, payload: dict[str, Any]) -> SlurmJob:
    base_dir = os.environ.get("SLURM_TASK_DIR", "/tmp/task_balancer")
    workdir = os.path.join(base_dir, task_id)
    os.makedirs(workdir, exist_ok=True)

    stdout_path = os.path.join(workdir, "stdout.txt")
    stderr_path = os.path.join(workdir, "stderr.txt")
    result_path = os.path.join(workdir, "result.json")

    # Команда, которую выполнит Slurm job:
    # пишем result.json и делаем sleep
    job_cmd = [
        "bash",
        "-lc",
        (
            f"set -euo pipefail; "
            f"sleep {int(sleep_s)}; "
            f"python3 - <<'PY'\n"
            f"import json\n"
            f"payload = {json.dumps(payload)}\n"
            f"out = {{'ok': True, 'task_type': 'demo_sleep', 'slept': {int(sleep_s)}, 'echo': payload}}\n"
            f"open('{result_path}', 'w').write(json.dumps(out))\n"
            f"print('WROTE_RESULT')\n"
            f"PY\n"
        ),
    ]

    # sbatch: делаем “inline” через --wrap
    # --parsable возвращает job_id
    submit_cmd = [
        "sbatch",
        "--parsable",
        "--job-name", f"taskbal_{task_id[:8]}",
        "--output", stdout_path,
        "--error", stderr_path,
        "--wrap", " ".join(job_cmd),
    ]

    job_id = _run(submit_cmd)
    return SlurmJob(job_id=job_id, workdir=workdir, stdout_path=stdout_path, stderr_path=stderr_path, result_path=result_path)


def get_job_state(job_id: str) -> tuple[str, Optional[int]]:
    # sacct бывает не сразу видит job => fallback state=UNKNOWN
    try:
        out = _run(["sacct", "-j", job_id, "--format=State,ExitCode", "-n", "-P"])
        # обычно строк несколько, берём первую непустую
        line = next((l for l in out.splitlines() if l.strip()), "")
        if not line:
            return ("UNKNOWN", None)
        state, exitcode = line.split("|", 1)
        # ExitCode вида "0:0"
        code = int(exitcode.split(":")[0]) if exitcode else None
        return (state.strip(), code)
    except Exception:
        return ("UNKNOWN", None)


def read_json_file(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_text_file(path: str, max_chars: int = 8000) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read(max_chars)
    except FileNotFoundError:
        return ""
