import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

HOSTS = ["bastion", "worker1", "worker2"]

SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=5",
    "-o", "ServerAliveCountMax=2",
]

REPO_DIR = "~/task_balancer"

REQUIRED_KEYS = [
    "proc_total",
    "proc_runnable",
    "default_iface",
    "net_default",
]


def run(cmd: List[str]) -> Tuple[int, str, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, shell=False)
    return p.returncode, p.stdout, p.stderr


def ssh(host: str, remote_cmd: str) -> Tuple[int, str, str]:
    return run(["ssh", *SSH_OPTS, host, remote_cmd])


def pull_repo(host: str) -> Dict:
    t0 = time.time()
    cmd = (
        "bash -lc '"
        f"set -e; cd {REPO_DIR}; "
        "git pull --ff-only"
        "'"
    )
    code, out, err = ssh(host, cmd)
    return {
        "host": host,
        "ok": code == 0,
        "elapsed": round(time.time() - t0, 2),
        "out": out.strip(),
        "err": err.strip(),
    }


def run_agent(host: str) -> Dict:
    t0 = time.time()
    cmd = (
        "bash -lc '"
        f"set -e; cd {REPO_DIR}; "
        "python3 remote/agent_status.py"
        "'"
    )
    code, out, err = ssh(host, cmd)
    dt = round(time.time() - t0, 2)

    if code != 0 or not out.strip():
        return {"host": host, "ok": False, "elapsed": dt, "error": (err.strip() or out.strip())}

    raw = out.strip().splitlines()[-1].strip()
    try:
        data = json.loads(raw)
    except Exception as e:
        return {"host": host, "ok": False, "elapsed": dt, "error": f"JSON parse error: {e}", "raw": raw[:500]}

    missing = [k for k in REQUIRED_KEYS if k not in data]
    return {
        "host": host,
        "ok": len(missing) == 0,
        "elapsed": dt,
        "missing": missing,
        "hostname": data.get("hostname"),
        "ip": data.get("primary_ip"),
        "proc_runnable": data.get("proc_runnable"),
        "default_iface": data.get("default_iface"),
        "net_default": data.get("net_default"),
    }


def main() -> int:
    print("== STEP 1: git pull on all nodes ==")
    pulls = []
    with ThreadPoolExecutor(max_workers=len(HOSTS)) as ex:
        futs = [ex.submit(pull_repo, h) for h in HOSTS]
        for f in as_completed(futs):
            pulls.append(f.result())
    pulls.sort(key=lambda x: x["host"])
    for p in pulls:
        status = "OK" if p["ok"] else "FAIL"
        print(f"{p['host']:<8} {status:<4} t={p['elapsed']}s  {p['out'] or p['err']}")
    print()

    print("== STEP 2: run agent + verify keys ==")
    agents = []
    with ThreadPoolExecutor(max_workers=len(HOSTS)) as ex:
        futs = [ex.submit(run_agent, h) for h in HOSTS]
        for f in as_completed(futs):
            agents.append(f.result())
    agents.sort(key=lambda x: x["host"])

    print(f"{'HOST':<8} {'OK':<3} {'HOSTNAME':<10} {'IP':<13} {'R':>3} {'IFACE':<8} {'NET(rx/tx)':<18} {'MISSING':<20} t(s)")
    print("-" * 90)
    for a in agents:
        if not a["ok"]:
            miss = ",".join(a.get("missing", [])) or a.get("error", "")
            print(f"{a['host']:<8} NO  {'-':<10} {'-':<13} {'-':>3} {'-':<8} {'-':<18} {miss[:20]:<20} {a['elapsed']}")
            continue
        nd = a.get("net_default") or {}
        rx = nd.get("rx_bytes")
        tx = nd.get("tx_bytes")
        net = f"{rx}/{tx}" if rx is not None and tx is not None else "-"
        miss = ",".join(a.get("missing", []))
        print(f"{a['host']:<8} YES {str(a.get('hostname','-')):<10} {str(a.get('ip','-')):<13} {str(a.get('proc_runnable','-')):>3} {str(a.get('default_iface','-')):<8} {net:<18} {miss:<20} {a['elapsed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())