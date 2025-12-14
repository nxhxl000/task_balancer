#!/usr/bin/env python3
import json
import os
import shutil
import socket
import time
from pathlib import Path


def read_first_line(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()[0].strip()
    except Exception:
        return ""


def cpu_usage_pct(sample: float = 0.35):
    def read_cpu():
        try:
            with open("/proc/stat", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("cpu "):
                        return list(map(int, line.split()[1:]))
        except Exception:
            return None
        return None

    a = read_cpu()
    if not a:
        return None
    time.sleep(sample)
    b = read_cpu()
    if not b:
        return None

    idle_a = a[3] + (a[4] if len(a) > 4 else 0)
    idle_b = b[3] + (b[4] if len(b) > 4 else 0)

    total_a = sum(a)
    total_b = sum(b)
    total_d = total_b - total_a
    idle_d = idle_b - idle_a

    if total_d <= 0:
        return None

    usage = (1.0 - (idle_d / total_d)) * 100.0
    usage = max(0.0, min(100.0, usage))
    return round(usage, 2)


def loadavg():
    try:
        with open("/proc/loadavg", "r", encoding="utf-8") as f:
            p = f.read().split()
            return {"load1": float(p[0]), "load5": float(p[1]), "load15": float(p[2])}
    except Exception:
        return {"load1": None, "load5": None, "load15": None}


def uptime_seconds():
    try:
        with open("/proc/uptime", "r", encoding="utf-8") as f:
            return float(f.read().split()[0])
    except Exception:
        return None


def meminfo_bytes():
    data = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if ":" not in line:
                    continue
                k, v = line.split(":", 1)
                v = v.strip().split()[0]
                try:
                    data[k] = int(v) * 1024
                except Exception:
                    pass
    except Exception:
        pass

    total = data.get("MemTotal")
    avail = data.get("MemAvailable")
    used = (total - avail) if (total is not None and avail is not None) else None
    return {"mem_total_bytes": total, "mem_available_bytes": avail, "mem_used_bytes": used}


def swapinfo_bytes():
    data = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("SwapTotal") or line.startswith("SwapFree"):
                    k, v = line.split(":", 1)
                    v = v.strip().split()[0]
                    try:
                        data[k] = int(v) * 1024
                    except Exception:
                        pass
    except Exception:
        pass

    total = data.get("SwapTotal")
    free = data.get("SwapFree")
    used = (total - free) if (total is not None and free is not None) else None
    return {"swap_total_bytes": total, "swap_free_bytes": free, "swap_used_bytes": used}


def disk_root():
    try:
        du = shutil.disk_usage("/")
        return {
            "disk_root_total_bytes": du.total,
            "disk_root_used_bytes": du.used,
            "disk_root_free_bytes": du.free,
        }
    except Exception:
        return {
            "disk_root_total_bytes": None,
            "disk_root_used_bytes": None,
            "disk_root_free_bytes": None,
        }


def primary_ip():
    # Надёжный способ узнать IP интерфейса по маршруту наружу
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("1.1.1.1", 53))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


def kernel_release():
    try:
        return os.uname().release
    except Exception:
        return None


def os_release_pretty():
    try:
        # /etc/os-release: PRETTY_NAME="Ubuntu 24.04.3 LTS"
        text = Path("/etc/os-release").read_text(encoding="utf-8", errors="ignore")
        for line in text.splitlines():
            if line.startswith("PRETTY_NAME="):
                v = line.split("=", 1)[1].strip().strip('"')
                return v
    except Exception:
        pass
    return None


def main():
    host = os.uname().nodename
    cpu_cores = os.cpu_count()

    info = {
        "hostname": host,
        "user": os.getenv("USER"),
        "utc_time": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "os": os_release_pretty(),
        "kernel": kernel_release(),
        "uptime_seconds": (round(uptime_seconds(), 2) if uptime_seconds() is not None else None),
        "primary_ip": primary_ip(),
        "cpu_cores": cpu_cores,
        "cpu_usage_pct": cpu_usage_pct(),
    }

    info.update(loadavg())
    info.update(meminfo_bytes())
    info.update(swapinfo_bytes())
    info.update(disk_root())

    # производные метрики (оценка "свободно")
    cpu_usage = info.get("cpu_usage_pct")
    if isinstance(cpu_usage, (int, float)):
        info["cpu_free_pct"] = round(100.0 - cpu_usage, 2)
    else:
        info["cpu_free_pct"] = None

    if cpu_cores and isinstance(info.get("cpu_free_pct"), (int, float)):
        info["cpu_free_cores_est"] = round(cpu_cores * (info["cpu_free_pct"] / 100.0), 2)
    else:
        info["cpu_free_cores_est"] = None

    info["mem_free_bytes"] = info.get("mem_available_bytes")
    info["disk_free_bytes"] = info.get("disk_root_free_bytes")

    print(json.dumps(info, ensure_ascii=False))


if __name__ == "__main__":
    main()