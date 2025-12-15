from typing import Any
import time


def execute_local(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    # Локальный backend — только для демо
    if task_type != "demo_sleep":
        raise NotImplementedError(
            f"Local backend supports only 'demo_sleep'. Got task_type={task_type}"
        )

    sleep_raw = payload.get("sleep_s", 1)
    try:
        sleep_s = int(sleep_raw)
    except (TypeError, ValueError):
        raise ValueError(f"payload.sleep_s must be int, got: {sleep_raw!r}")

    if sleep_s < 0 or sleep_s > 3600:
        raise ValueError(f"payload.sleep_s out of range (0..3600): {sleep_s}")

    time.sleep(sleep_s)

    return {
        "ok": True,
        "task_type": task_type,
        "slept": sleep_s,
        "echo": payload,
    }
