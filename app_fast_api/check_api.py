import os
import sys
import json
import time
from typing import Any, Dict

import requests


BASE_URL = os.environ.get("VITE_TASK_API_URL", "http://127.0.0.1:8000").rstrip("/")
TIMEOUT = 10


def pretty(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)


def assert_ok(resp: requests.Response, msg: str = ""):
    if not resp.ok:
        raise AssertionError(
            f"{msg}\nHTTP {resp.status_code}\nURL: {resp.url}\nBody:\n{resp.text}"
        )

def check_health():
    # 1) health
    print("\n[1] GET /health")
    r = requests.get(f"{BASE_URL}/health", timeout=TIMEOUT)
    assert_ok(r, "Health check failed")
    data = r.json()
    assert data.get("ok") is True, f"Unexpected /health response: {data}"
    print("OK:", data)

def check_create_task_latin_square():
     # 2) create task
    print("\n[2] POST /tasks (create)")
    new_task: Dict[str, Any] = {
        "task_type": "front_test_" + "latin_square_from_prefix",
        "n": 5,
        "priority": 50,
        "max_attempts": 10,
        "payload": {
            "problem": "complete_latin_square_from_prefix",
            "output": {
                "return_one_solution": True
            },
            "prefix_format": "matrix_nulls",
            "prefix": [
                [0, 1, 2, 3, 4],
                [None, None, None, None, None],
                [None, None, None, None, None],
                [None, None, None, None, None],
                [None, None, None, None, None],
            ],
            "constraints":{
                "latin": True,
                "symmetry_breaking":{
                    "fix_first_row": True
                    }
                },
            "prefix_format":"matrix_nulls"
        }
    }
    r = requests.post(f"{BASE_URL}/tasks", json=new_task, timeout=TIMEOUT)
    assert_ok(r, "Create task failed")
    created = r.json()
    print("Created:\n", pretty(created))
    return created, new_task

def check_create_task_mols_search():
     # 2) create task
    print("\n[2] POST /tasks (create)")
    new_task: Dict[str, Any] = {
        "task_type": "front_test_" + "mols_search",
        "n": 5,
        "priority": 50,
        "max_attempts": 10,
        "payload": {
            "k":2,
            "n":9,
            "seed":45203843,
            "budget":{
                "max_steps":2000000,
                "time_limit_sec":600
                },
            "method":"Jacobson-Matthews",
            "problem":"search_mols"
            },
    }
    r = requests.post(f"{BASE_URL}/tasks", json=new_task, timeout=TIMEOUT)
    assert_ok(r, "Create task failed")
    created = r.json()
    print("Created:\n", pretty(created))
    return created, new_task

def get_list_tasks():
    # 3) list tasks (filter by task_type)
    print("\n[3] GET /tasks?task_type=latin_square_from_prefix&limit=5")
    r = requests.get(
        f"{BASE_URL}/tasks",
        params={"task_type": "latin_square_from_prefix", "limit": 5},
        timeout=TIMEOUT,
    )
    assert_ok(r, "List tasks failed")
    lst = r.json()
    return lst

def get_created_task(task_id):
     # 4) get created task
    print("\n[4] GET /tasks/{id}")
    r = requests.get(f"{BASE_URL}/tasks/{task_id}", timeout=TIMEOUT)
    assert_ok(r, "Get task failed")
    got = r.json()
    return got

def change_task(task_id):
    # 5) patch task (set status running)
    print("\n[5] PATCH /tasks/{id} (status=running)")
    r = requests.patch(
        f"{BASE_URL}/tasks/{task_id}",
        json={"status": "running"},
        timeout=TIMEOUT,
    )
    assert_ok(r, "Patch task failed")
    patched = r.json()
    return patched

def cancel_task(task_id):
    # 7) cancel created task (если уже running, твой cancel не запрещает — он разрешает, пока не done/failed/canceled)
    print("\n[7] POST /tasks/{id}/cancel")
    r = requests.post(f"{BASE_URL}/tasks/{task_id}/cancel", timeout=TIMEOUT)
    assert_ok(r, "Cancel task failed")
    canceled = r.json()
    return canceled

def main():
    print(f"BASE_URL = {BASE_URL}")

    check_health()

    created, new_task = check_create_task_latin_square()

    task_id = created["id"]
    assert created["task_type"] == new_task["task_type"]
    assert created["n"] == new_task["n"]
    assert created["priority"] == new_task["priority"]

    lst = get_list_tasks()
    assert isinstance(lst, list), "List response is not a list"
    print(f"Listed {len(lst)} tasks (showing up to 5)")

    got = get_created_task(task_id)
    assert got["id"] == task_id
    print("Fetched:\n", pretty(got))
   
    patched = change_task(task_id)
    assert patched["status"] == "running"
    print("Patched:\n", pretty(patched))
    

    # # 6) lease one task (may lease some other queued task if exists)
    # print("\n[6] POST /tasks/lease")
    # lease_body = {"leased_by": "check_api.py", "lease_seconds": 60}
    # r = requests.post(f"{BASE_URL}/tasks/lease", json=lease_body, timeout=TIMEOUT)
    # if r.status_code == 404:
    #     print("No tasks available to lease (OK if queue empty).")
    # else:
    #     assert_ok(r, "Lease failed")
    #     leased = r.json()
    #     assert leased["status"] == "leased"
    #     assert leased["leased_by"] == lease_body["leased_by"]
    #     print("Leased:\n", pretty(leased))

    canceled = cancel_task(task_id)
    assert canceled["status"] == "canceled"
    print("Canceled:\n", pretty(canceled))

    # 8) check cancel again -> should be 409 (already canceled)
    print("\n[8] POST /tasks/{id}/cancel (again) -> expect 409")
    r = requests.post(f"{BASE_URL}/tasks/{task_id}/cancel", timeout=TIMEOUT)
    if r.status_code != 409:
        raise AssertionError(f"Expected 409, got {r.status_code}: {r.text}")
    print("OK: got 409 Conflict as expected")

    print("\n✅ ALL CHECKS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ CHECKS FAILED:", str(e))
        sys.exit(1)
