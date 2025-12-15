from __future__ import annotations
import argparse, json, sys, traceback
from pathlib import Path

from app.core.worker_local import execute_local  # можно переиспользовать твой execute_local

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--task-type", required=True)
    ap.add_argument("--payload-json", required=True)  # строка json
    ap.add_argument("--artifact-dir", required=True)
    args = ap.parse_args()

    art = Path(args.artifact_dir)
    art.mkdir(parents=True, exist_ok=True)

    try:
        payload = json.loads(args.payload_json)
        result = execute_local(args.task_type, payload)

        (art / "result.json").write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
        sys.exit(0)

    except Exception as e:
        err = f"{e}\n{traceback.format_exc()}"
        (art / "error.txt").write_text(err, encoding="utf-8")
        sys.exit(1)

if __name__ == "__main__":
    main()