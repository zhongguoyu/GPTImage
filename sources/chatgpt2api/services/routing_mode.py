"""Persisted administrator choice; switching is allowed only while idle.

The application runs one ASGI worker. Do not use multiple workers with this guard.
"""
import json
import os
import threading
from pathlib import Path
from fastapi import HTTPException

_lock = threading.RLock()
_active_requests = 0

def mode_path():
    from services.config import DATA_DIR
    return Path(DATA_DIR) / "routing-mode.json"

def explicit_mode():
    try:
        value = json.loads(mode_path().read_text(encoding="utf-8"))["mode"]
    except FileNotFoundError:
        return None
    except (ValueError, KeyError):
        raise HTTPException(503, detail="上游链路配置损坏，请恢复 routing-mode.json") from None
    if value not in {"native", "sub2api"}:
        raise HTTPException(503, detail="上游链路配置无效")
    return value

def sub2_enabled():
    mode = explicit_mode()
    return mode == "sub2api" if mode else os.environ.get("SUB2API_ONLY") == "1"

def images_enabled():
    mode = explicit_mode()
    if mode:
        return mode == "sub2api"
    return bool(os.environ.get("IMAGE_UPSTREAM_CONFIG"))

def native_selected():
    return explicit_mode() == "native"

def state():
    mode = explicit_mode()
    return {"mode": mode or ("sub2api" if sub2_enabled() else "native"),
            "explicit": mode is not None,
            "image_upstream": images_enabled()}

def _busy_tasks():
    from services.image_task_service import image_task_service
    from services.editable_file_task_service import editable_file_task_service
    for service in (image_task_service, editable_file_task_service):
        with service._lock:
            if any(task.get("status") in {"queued", "running", "pending"}
                   for task in service._tasks.values()):
                return True
    return False

def switch(mode):
    if mode not in {"native", "sub2api"}:
        raise HTTPException(400, detail="mode 必须为 native 或 sub2api")
    with _lock:
        if _active_requests or _busy_tasks():
            raise HTTPException(409, detail="当前有请求或任务正在执行，请完成后再切换")
        if mode == "sub2api":
            from services.sub2_gateway import settings
            try:
                settings()
            except (OSError, ValueError):
                raise HTTPException(400, detail="Sub2API 上游配置文件不可用") from None
        path = mode_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"mode": mode}) + "\n", encoding="utf-8")
        os.replace(temporary, path)
        from services.protocol.chat_completion_cache import chat_completion_cache
        chat_completion_cache.clear()
        return state()

class RoutingActivityMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        global _active_requests
        path = scope.get("path", "")
        tracked = (scope["type"] == "http" and
                   path != "/api/routing-mode" and
                   (path.startswith("/v1/") or path.startswith("/api/")))
        if tracked:
            with _lock:
                _active_requests += 1
        try:
            await self.app(scope, receive, send)
        finally:
            if tracked:
                with _lock:
                    _active_requests -= 1
