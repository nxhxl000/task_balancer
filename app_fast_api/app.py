import os
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import set_json_dumps, set_json_loads, Json

from fastapi.middleware.cors import CORSMiddleware


# ---------- Config ----------
# Загружаем переменные окружения из .env (DATABASE_URL)
load_dotenv()

# DSN для подключения к Postgres (Neon / локальный Postgres и т.д.)
DSN = os.environ.get("DATABASE_URL")
if not DSN:
    raise RuntimeError("DATABASE_URL is not set. Create .env and set DATABASE_URL=...")

# Настраиваем psycopg3 так, чтобы JSON/JSONB автоматически:
# - из Python dict -> JSON при записи
# - из JSONB -> Python dict при чтении
set_json_dumps(lambda obj: __import__("json").dumps(obj, ensure_ascii=False))
set_json_loads(__import__("json").loads)

# Создаём приложение FastAPI
app = FastAPI(title="Tasks API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_conn():
    """
    Создаёт подключение к БД для одного запроса.
    Сейчас это "simple approach": каждый HTTP запрос создаёт отдельное соединение.

    Позже можно заменить на пул соединений (psycopg_pool.ConnectionPool),
    если вырастет нагрузка / частота запросов.
    """
    return psycopg.connect(DSN, row_factory=dict_row)


# ---------- Models ----------
class TaskStatus(str, Enum):
    """
    Возможные статусы задачи.

    queued   — задача в очереди и готова к выдаче воркеру
    leased   — задача выдана (зарезервирована) воркеру на время lease
    running  — воркер реально выполняет задачу
    done     — выполнено успешно, результат в result
    failed   — ошибка выполнения, описание в error
    canceled — отменено (не выполнять)
    """
    queued = "queued"
    leased = "leased"
    running = "running"
    done = "done"
    failed = "failed"
    canceled = "canceled"


class TaskCreate(BaseModel):
    """
    Тело запроса для создания новой задачи.

    task_type  — строковый идентификатор типа задачи (например: latin_square_from_prefix, mols_search)
    n          — размерность задачи (часто равна n в латинском квадрате или MOLS)
    priority   — приоритет (чем выше, тем быстрее будет взята в lease)
    max_attempts — сколько раз можно пытаться выполнить (для ретраев)
    payload    — JSON с деталями задачи
    """
    task_type: str = Field(..., examples=["front_test_latin_square_from_prefix", "front_test_mols_search"])
    n: int = Field(..., gt=0)
    priority: int = Field(100)
    max_attempts: int = Field(10, gt=0)
    payload: Dict[str, Any]


class TaskPatch(BaseModel):
    """
    Тело PATCH-запроса для частичного обновления задачи.

    Можно обновлять только те поля, которые переданы (остальные останутся как есть).
    Обычно воркеры будут менять:
    - status (leased -> running -> done/failed)
    - result или error
    - lease_expires_at (если делаете продление lease)
    """
    status: Optional[TaskStatus] = None
    leased_by: Optional[str] = None
    lease_expires_at: Optional[datetime] = None
    attempts: Optional[int] = Field(None, ge=0)
    max_attempts: Optional[int] = Field(None, gt=0)

    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class TaskOut(BaseModel):
    """
    Модель ответа (как задача выглядит с точки зрения API).

    Совпадает с таблицей tasks: id, task_type, статус, параметры, payload/result/error,
    а также created_at/updated_at.
    """
    id: uuid.UUID
    task_type: str
    status: TaskStatus

    n: int
    priority: int
    attempts: int
    max_attempts: int

    leased_by: Optional[str] = None
    lease_expires_at: Optional[datetime] = None

    payload: Dict[str, Any]
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    created_at: datetime
    updated_at: datetime


class LeaseRequest(BaseModel):
    """
    Тело запроса на "выдачу" (lease) задачи воркеру.

    leased_by      — идентификатор воркера (hostname, container_id, uuid и т.п.)
    lease_seconds  — на сколько секунд "блокируем" задачу за воркером.
                    Если воркер умрёт — после истечения lease задача снова станет доступна.
    """
    leased_by: str
    lease_seconds: int = Field(120, gt=0)


# ---------- Routes ----------
@app.get("/health")
def health():
    """
    Health-check эндпоинт.
    Используется для проверки, что сервис жив.
    """
    return {"ok": True}


@app.post("/tasks", response_model=TaskOut)
def create_task(body: TaskCreate):
    """
    Создать новую задачу в БД.

    Важно: payload — это dict, поэтому для psycopg3 нужно завернуть его в Json(...),
    иначе будет ошибка "cannot adapt type 'dict'".
    """
    task_id = uuid.uuid4()

    sql = """
    INSERT INTO public.tasks (id, task_type, n, priority, max_attempts, payload)
    VALUES (%s, %s, %s, %s, %s, %s)
    RETURNING *;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    task_id,
                    body.task_type,
                    body.n,
                    body.priority,
                    body.max_attempts,
                    Json(body.payload),  # корректная запись dict -> jsonb
                ),
            )
            return cur.fetchone()


@app.get("/tasks", response_model=List[TaskOut])
def list_tasks(
    status: Optional[TaskStatus] = Query(None),
    task_type: Optional[str] = Query(None),
    n: Optional[int] = Query(None, gt=0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    order: str = Query("created_at_desc", pattern="^(created_at_desc|created_at_asc|priority_desc)$"),
):
    """
    Получить список задач с фильтрами и пагинацией.

    Фильтры:
    - status: отдать задачи конкретного статуса
    - task_type: только задачи определённого типа
    - n: только задачи с указанным n

    Пагинация:
    - limit/offset

    Сортировка:
    - created_at_desc (по умолчанию)
    - created_at_asc
    - priority_desc (приоритет + дата)
    """
    where = []
    params: List[Any] = []

    if status is not None:
        where.append("status = %s")
        params.append(status.value)
    if task_type is not None:
        where.append("task_type = %s")
        params.append(task_type)
    if n is not None:
        where.append("n = %s")
        params.append(n)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    if order == "created_at_asc":
        order_sql = "ORDER BY created_at ASC"
    elif order == "priority_desc":
        order_sql = "ORDER BY priority DESC, created_at ASC"
    else:
        order_sql = "ORDER BY created_at DESC"

    sql = f"""
    SELECT * FROM public.tasks
    {where_sql}
    {order_sql}
    LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


@app.get("/tasks/{task_id}", response_model=TaskOut)
def get_task(task_id: uuid.UUID):
    """
    Получить одну задачу по её id.

    404, если задачи нет.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM public.tasks WHERE id = %s;", (task_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Task not found")
            return row


@app.patch("/tasks/{task_id}", response_model=TaskOut)
def patch_task(task_id: uuid.UUID, body: TaskPatch):
    """
    Частично обновить задачу по id (PATCH).

    Обновляются только те поля, которые переданы в body.
    Если ничего не передано — вернём 400.

    Примечание:
    - result (dict) обязательно заворачиваем в Json(...), чтобы psycopg3 записал jsonb корректно.
    """
    fields = []
    params: List[Any] = []

    # Собираем динамический UPDATE только из переданных полей
    if body.status is not None:
        fields.append("status = %s")
        params.append(body.status.value)
    if body.leased_by is not None:
        fields.append("leased_by = %s")
        params.append(body.leased_by)
    if body.lease_expires_at is not None:
        fields.append("lease_expires_at = %s")
        params.append(body.lease_expires_at)
    if body.attempts is not None:
        fields.append("attempts = %s")
        params.append(body.attempts)
    if body.max_attempts is not None:
        fields.append("max_attempts = %s")
        params.append(body.max_attempts)
    if body.result is not None:
        fields.append("result = %s")
        params.append(Json(body.result))  # dict -> jsonb
    if body.error is not None:
        fields.append("error = %s")
        params.append(body.error)

    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(task_id)

    sql = f"""
    UPDATE public.tasks
    SET {", ".join(fields)}
    WHERE id = %s
    RETURNING *;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Task not found")
            return row


@app.post("/tasks/lease", response_model=TaskOut)
def lease_one_task(body: LeaseRequest):
    """
    Выдать (lease) одну задачу воркеру атомарно.

    Логика:
    - берём задачу со статусом 'queued'
      или задачу 'leased' у которой lease_expires_at < now() (lease истёк)
    - сортировка: priority DESC, created_at ASC
    - FOR UPDATE SKIP LOCKED гарантирует, что параллельные воркеры не возьмут одну и ту же задачу
    - выставляем:
        status = 'leased'
        leased_by = <кто взял>
        lease_expires_at = now() + lease_seconds
        attempts += 1 (только если задача была queued)

    Если доступных задач нет — возвращаем 404.
    """
    lease_sql = """
    WITH candidate AS (
      SELECT id
      FROM public.tasks
      WHERE
        status = 'queued'
        OR (status = 'leased' AND lease_expires_at < now())
      ORDER BY priority DESC, created_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE public.tasks t
    SET
      status = 'leased',
      leased_by = %s,
      lease_expires_at = now() + (%s::int || ' seconds')::interval,
      attempts = CASE WHEN t.status = 'queued' THEN t.attempts + 1 ELSE t.attempts END
    FROM candidate
    WHERE t.id = candidate.id
    RETURNING t.*;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(lease_sql, (body.leased_by, body.lease_seconds))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="No tasks available to lease")
            return row


@app.post("/tasks/{task_id}/cancel", response_model=TaskOut)
def cancel_task(task_id: uuid.UUID):
    """
    Отменить задачу (status -> canceled).

    Ограничение:
    - если задача уже в финальном статусе (done/failed/canceled) — возвращаем 409
    - если задачи нет — 404

    Сейчас отмена разрешена даже если задача running/leased/queued.
    При желании можно ужесточить правило (например, запрещать cancel если running).
    """
    sql = """
    UPDATE public.tasks
    SET status = 'canceled'
    WHERE id = %s AND status NOT IN ('done','failed','canceled')
    RETURNING *;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (task_id,))
            row = cur.fetchone()

            if not row:
                # либо нет задачи, либо она уже финальная — уточняем
                cur.execute("SELECT * FROM public.tasks WHERE id = %s;", (task_id,))
                existing = cur.fetchone()
                if not existing:
                    raise HTTPException(status_code=404, detail="Task not found")
                raise HTTPException(status_code=409, detail="Task already finished/canceled")

            return row
