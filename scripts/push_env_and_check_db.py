import base64
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


DEFAULT_HOSTS = ["bastion", "worker1", "worker2"]
REMOTE_ENV_PATH = "~/task_balancer/.env"


def run(cmd: List[str], check: bool = True) -> Tuple[int, str, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, shell=False)
    if check and p.returncode != 0:
        raise RuntimeError(
            f"Command failed ({p.returncode}): {' '.join(cmd)}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}"
        )
    return p.returncode, p.stdout, p.stderr


def find_repo_root(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(8):  # достаточно для обычной структуры
        if (cur / ".env").exists() or (cur / ".git").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return start.resolve()


def read_database_url_from_env_file(env_path: Path) -> str:
    if not env_path.exists():
        raise SystemExit(f"Не найден локальный .env: {env_path}")

    for raw in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith("DATABASE_URL"):
            continue
        _, val = line.split("=", 1)
        val = val.strip()
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        if not val:
            raise SystemExit("DATABASE_URL в .env найден, но пустой.")
        return val

    raise SystemExit("В локальном .env не найден ключ DATABASE_URL=")


def ssh(host: str, remote_cmd: str, timeout_sec: int = 10) -> Tuple[int, str, str]:
    # Добавили ServerAlive*, чтобы ssh не висел бесконечно
    cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={timeout_sec}",
        "-o",
        "ServerAliveInterval=5",
        "-o",
        "ServerAliveCountMax=2",
        host,
        remote_cmd,
    ]
    return run(cmd, check=False)


def remote_write_env_cmd(env_b64: str) -> str:
    # Пишем через python3 на удалённой машине (самый стабильный способ)
    return (
        "bash -lc "
        + "'"
        + 'python3 -c "import base64, pathlib; '
          f'p=pathlib.Path(\\"{REMOTE_ENV_PATH}\\").expanduser(); '
          'p.parent.mkdir(parents=True, exist_ok=True); '
          f'p.write_text(base64.b64decode(\\"{env_b64}\\").decode(\\"utf-8\\")); '
          'p.chmod(0o600); '
          'print(str(p))"'
        + "'"
    )


def remote_check_db_cmd() -> str:
    # - sudo -n: не спрашивать пароль (если нет прав — сразу ошибка)
    # - DEBIAN_FRONTEND=noninteractive: без диалогов
    # - PGCONNECT_TIMEOUT=8: чтобы psql не зависал при сетевых проблемах
    # - psql -X: не читать ~/.psqlrc (иногда мешает)
    return (
        "bash -lc "
        + "'"
        + "set -e; "
        + "if ! command -v psql >/dev/null 2>&1; then "
        + "  if sudo -n true 2>/dev/null; then "
        + "    sudo -n apt-get update -y >/dev/null; "
        + "    sudo -n DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-client >/dev/null; "
        + "  else "
        + "    echo \"ERROR: psql not found and sudo without password is not available.\"; "
        + "    echo \"Fix: run on host: sudo apt-get update && sudo apt-get install -y postgresql-client\"; "
        + "    exit 2; "
        + "  fi; "
        + "fi; "
        + f"set -a; source {REMOTE_ENV_PATH}; set +a; "
        + "export PGCONNECT_TIMEOUT=8; "
        + 'psql -X "$DATABASE_URL" -c "select now();"'
        + "'"
    )


def main() -> int:
    # 1) Берём DATABASE_URL из локального .env в корне репы
    repo_root = find_repo_root(Path(__file__).parent)
    local_env = repo_root / ".env"
    db_url = read_database_url_from_env_file(local_env)

    env_text = f'DATABASE_URL="{db_url}"\n'
    env_b64 = base64.b64encode(env_text.encode("utf-8")).decode("ascii")

    print(f"Local .env: {local_env}")
    print("Targets:", ", ".join(DEFAULT_HOSTS))
    print()

    for host in DEFAULT_HOSTS:
        print(f"=== {host} ===")

        code, out, err = ssh(host, remote_write_env_cmd(env_b64))
        if code != 0:
            print("❌ Failed to write remote .env")
            print((out + "\n" + err).strip())
            print()
            continue

        print(f"✅ .env written: {out.strip()}")

        code, out, err = ssh(host, remote_check_db_cmd())
        if code != 0:
            print("❌ DB check failed")
            print((out + "\n" + err).strip())
            print()
            continue

        print("✅ DB check OK:")
        print(out.strip())
        print()

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
