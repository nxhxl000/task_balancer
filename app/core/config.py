import os

def load_env() -> None:
    # мягкая загрузка .env, если библиотека есть
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass

def get_database_url() -> str:
    load_env()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is missing. Put it into .env (see env.example).")
    return dsn