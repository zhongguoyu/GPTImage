"""Persisted administrator choice; switching is allowed only while idle.

The application runs one ASGI worker. Do not use multiple workers with this guard.
"""
import json
import os
import threading
import time
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from fastapi import HTTPException

_lock = threading.RLock()
_active_requests = 0

def mode_path():
    from services.config import DATA_DIR
    return Path(DATA_DIR) / "routing-mode.json"

def switched_at():
    try:
        return float(json.loads(mode_path().read_text(encoding="utf-8")).get("switched_at", 0))
    except FileNotFoundError:
        return 0

@contextmanager
def account_activity():
    global _active_requests
    with _lock:
        _active_requests += 1
    try:
        yield
    finally:
        with _lock:
            _active_requests -= 1

def guard_account_activity(fn):
    @wraps(fn)
    def guarded(*args, **kwargs):
        with account_activity():
            return fn(*args, **kwargs)
    return guarded

def switch_and_refresh(mode):
    # Reserve the guard before releasing the switch lock; worker threads can then
    # refresh concurrently, while another switch cannot mix metadata sources.
    global _active_requests
    with _lock:
        result = switch(mode)
        _active_requests += 1
    try:
        from services.account_service import account_service
        refreshed = account_service.refresh_accounts(account_service.list_tokens(), quiet=True)
        result["account_refresh"] = {"refreshed": refreshed.get("refreshed", 0),
                                     "failed": len(refreshed.get("errors", []))}
    except Exception:
        # Routing has already changed. Do not claim the switch failed or expose
        # upstream errors containing credentials; old metadata remains stale.
        result["account_refresh"] = {"error": "账号同步失败，请在号池管理重试刷新"}
    finally:
        with _lock:
            _active_requests -= 1
    return result

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
        temporary.write_text(json.dumps({"mode": mode, "switched_at": time.time()}) + "\n", encoding="utf-8")
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
