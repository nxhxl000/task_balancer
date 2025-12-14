import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple


HOSTS = ["bastion", "worker1", "worker2"]

REPO_DIR = "~/task_balancer"
REMOTE_AGENT = "remote/agent_status.py"

OUT_JSON = Path("infra/nodes_status.json")

SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=5",
    "-o", "ServerAliveCountMax=2",
]


def run(cmd: List[str]) -> Tuple[int, str, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, shell=False)
    return p.returncode, p.stdout, p.stderr


def ssh(host: str, remote_cmd: str) -> Tuple[int, str, str]:
    return run(["ssh", *SSH_OPTS, host, remote_cmd])


def collect_one(host: str) -> Dict:
    t0 = time.time()
    remote_cmd = (
        "bash -lc '"
        f"set -e; cd {REPO_DIR}; "
        f"python3 {REMOTE_AGENT}"
        "'"
    )
    code, out, err = ssh(host, remote_cmd)
    dt = round(time.time() - t0, 2)

    if code != 0 or not out.strip():
        return {
            "host_alias": host,
            "ok": False,
            "elapsed_sec": dt,
            "error": (err.strip() or out.strip() or f"ssh exit={code}"),
        }

    raw = out.strip()
    raw_line = raw.splitlines()[-1].strip()

    try:
        data = json.loads(raw_line)
        data["host_alias"] = host
        data["ok"] = True
        data["elapsed_sec"] = dt
        return data
    except Exception as e:
        return {
            "host_alias": host,
            "ok": False,
            "elapsed_sec": dt,
            "error": f"JSON parse error: {e}",
            "raw": raw[:1500],
        }


def fmt_bytes(n: int) -> str:
    if n is None:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        if x < 1024 or u == units[-1]:
            return f"{x:.1f}{u}"
        x /= 1024
    return f"{x:.1f}TB"


def fmt_num(x, digits=2) -> str:
    if x is None:
        return "-"
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return str(x)


def fmt_net_mb(net_default: Dict) -> str:
    """
    Показывает rx/tx в MB по default iface (счётчики с момента старта).
    """
    if not isinstance(net_default, dict):
        return "-/-"
    rx = net_default.get("rx_bytes")
    tx = net_default.get("tx_bytes")
    if rx is None or tx is None:
        return "-/-"
    rx_mb = rx / (1024 * 1024)
    tx_mb = tx / (1024 * 1024)
    return f"{rx_mb:.0f}/{tx_mb:.0f}"


def main() -> int:
    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=min(8, len(HOSTS))) as ex:
        futs = {ex.submit(collect_one, h): h for h in HOSTS}
        for f in as_completed(futs):
            results.append(f.result())

    results.sort(key=lambda r: r.get("host_alias", ""))

    header = (
        f"{'ALIAS':<8} {'HOSTNAME':<10} {'IP':<13} "
        f"{'CPU%':>6} {'FREE%':>7} {'FREEc':>7} "
        f"{'RAM_av':>10} {'DISK_fr':>10} {'LOAD1':>6} "
        f"{'R':>3} {'NET(MB)':>8} {'OK':>4} {'t(s)':>5}"
    )
    print("\nSUMMARY:")
    print(header)
    print("-" * len(header))

    for r in results:
        if not r.get("ok"):
            print(
                f"{r.get('host_alias','-'):<8} {'-':<10} {'-':<13} "
                f"{'-':>6} {'-':>7} {'-':>7} "
                f"{'-':>10} {'-':>10} {'-':>6} "
                f"{'-':>3} {'-/-':>8} "
                f"{'NO':>4} {fmt_num(r.get('elapsed_sec'),1):>5}  {r.get('error','')}"
            )
            continue

        proc_r = r.get("proc_runnable")
        net_mb = fmt_net_mb(r.get("net_default"))

        print(
            f"{r.get('host_alias','-'):<8} "
            f"{r.get('hostname','-'):<10} "
            f"{(r.get('primary_ip') or '-'): <13} "
            f"{fmt_num(r.get('cpu_usage_pct'),2):>6} "
            f"{fmt_num(r.get('cpu_free_pct'),2):>7} "
            f"{fmt_num(r.get('cpu_free_cores_est'),2):>7} "
            f"{fmt_bytes(r.get('mem_available_bytes')):>10} "
            f"{fmt_bytes(r.get('disk_root_free_bytes')):>10} "
            f"{fmt_num(r.get('load1'),2):>6} "
            f"{(str(proc_r) if proc_r is not None else '-'):>3} "
            f"{net_mb:>8} "
            f"{'YES':>4} "
            f"{fmt_num(r.get('elapsed_sec'),1):>5}"
        )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved: {OUT_JSON.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())