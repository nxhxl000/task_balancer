from __future__ import annotations

import hmac
import hashlib
import json
import os
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from app.core.queue import mark_done, mark_failed

app = FastAPI()


class ResultIn(BaseModel):
    task_id: str
    leased_by: str
    ok: bool
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None


def _get_secret() -> bytes:
    # читаем секрет каждый раз (чтобы не зависеть от момента старта uvicorn)
    return os.environ.get("RESULT_SECRET", "").encode("utf-8")


def verify_sig(body: bytes, sig_hex: str) -> bool:
    secret = _get_secret()
    if not secret:
        return False
    mac = hmac.new(secret, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, sig_hex)


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/v1/task-result")
async def task_result(request: Request, x_task_sig: str = Header(default="")):
    body = await request.body()

    if not x_task_sig or not verify_sig(body, x_task_sig):
        raise HTTPException(status_code=401, detail="bad signature")

    data = json.loads(body.decode("utf-8"))
    payload = ResultIn(**data)

    if payload.ok:
        mark_done(payload.task_id, payload.leased_by, payload.result or {"ok": True})
        return {"ok": True, "status": "done"}

    mark_failed(payload.task_id, payload.leased_by, payload.error or "unknown error", retry=False)
    return {"ok": True, "status": "failed"}
