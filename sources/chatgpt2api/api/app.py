from __future__ import annotations

from contextlib import asynccontextmanager
from threading import Event

from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from api import accounts, ai, image_tasks, system
from api.errors import install_exception_handlers
from api.support import resolve_web_asset, start_limited_account_watcher
from services.backup_service import backup_service
from services.auth_service import auth_service
from services.config import config
from services.editable_file_task_service import editable_file_task_service
from services.image_service import start_image_cleanup_scheduler
from services.image_task_service import image_task_service
from services.log_service import log_service
from services.quota_service import quota_service
from services.scheduler.service import scheduler_service
from services.sub2api_auto_sync_service import sub2api_auto_sync_service


def create_app() -> FastAPI:
    app_version = config.app_version

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        stop_event = Event()
        await run_in_threadpool(log_service.initialize)
        await run_in_threadpool(log_service.cleanup_expired, config.log_retention_days)
        await run_in_threadpool(quota_service.initialize)
        await run_in_threadpool(quota_service.ensure_user_keys, auth_service.list_keys(role="user"))
        quota_watchdog = quota_service.start_watchdog(stop_event)
        await scheduler_service.start()
        await image_tasks.image_task_service.start()
        sub2api_auto_sync_service.start(stop_event)
        log_cleanup_thread = log_service.start_retention_scheduler(
            stop_event,
            lambda: config.log_retention_days,
        )
        thread = start_limited_account_watcher(stop_event)
        cleanup_thread = start_image_cleanup_scheduler(stop_event)
        backup_service.start()
        config.cleanup_old_images()
        try:
            yield
        finally:
            stop_event.set()
            sub2api_auto_sync_service.stop(timeout=2)
            await scheduler_service.stop()
            image_task_service.shutdown(wait=False)
            editable_file_task_service.shutdown(wait=False)
            quota_watchdog.join(timeout=1)
            log_cleanup_thread.join(timeout=1)
            thread.join(timeout=1)
            cleanup_thread.join(timeout=1)
            backup_service.stop()
            quota_service.close()
            log_service.close()
            storage = getattr(config, "_storage_backend", None)
            if storage is not None:
                close_storage = getattr(storage, "close", None)
                if callable(close_storage):
                    close_storage()

    app = FastAPI(title="chatgpt2api", version=app_version, lifespan=lifespan)
    install_exception_handlers(app)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(ai.create_router())
    app.include_router(accounts.create_router())
    app.include_router(image_tasks.create_router())
    app.include_router(system.create_router(app_version))

    @app.api_route("/{full_path:path}", methods=["GET", "HEAD"], include_in_schema=False)
    async def serve_web(full_path: str):
        asset = resolve_web_asset(full_path)
        if asset is not None:
            return FileResponse(asset)
        if full_path.strip("/").startswith("_next/"):
            raise HTTPException(status_code=404, detail="Not Found")
        fallback = resolve_web_asset("")
        if fallback is None:
            raise HTTPException(status_code=404, detail="Not Found")
        return FileResponse(fallback)

    return app
