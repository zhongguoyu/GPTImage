from __future__ import annotations

import uuid
from pathlib import Path
from threading import Event, Thread
from typing import Any

from fastapi import HTTPException, Request

from services.account_service import account_service
from services.auth_service import auth_service
from services.config import config
from services.quota_service import QuotaAccounting, QuotaExceededError, QuotaType, quota_service
from services.user_key_concurrency import user_key_concurrency
from utils.log import logger

BASE_DIR = Path(__file__).resolve().parents[1]
WEB_DIST_DIR = BASE_DIR / "web_dist"


def extract_bearer_token(authorization: str | None) -> str:
    scheme, _, value = str(authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not value.strip():
        return ""
    return value.strip()


def _legacy_admin_identity(token: str) -> dict[str, object] | None:
    auth_key = str(config.auth_key or "").strip()
    if auth_key and token == auth_key:
        return {"id": "admin", "name": "管理员", "role": "admin"}
    return None


def _require_identity(authorization: str | None, *, web_login: bool = False) -> dict[str, object]:
    token = extract_bearer_token(authorization)
    if web_login:
        identity = _legacy_admin_identity(token) or auth_service.authenticate_web_login(token)
    else:
        identity = _legacy_admin_identity(token) or auth_service.authenticate(token)
    if identity is None:
        raise HTTPException(status_code=401, detail={"error": "密钥无效或已失效，请重新登录"})
    return identity


def require_identity(authorization: str | None) -> dict[str, object]:
    return _require_identity(authorization)


def require_web_identity(authorization: str | None) -> dict[str, object]:
    return _require_identity(authorization, web_login=True)


def require_auth_key(authorization: str | None) -> None:
    require_identity(authorization)


def require_admin(authorization: str | None) -> dict[str, object]:
    identity = require_identity(authorization)
    if identity.get("role") != "admin":
        raise HTTPException(status_code=403, detail={"error": "需要管理员权限才能执行这个操作"})
    return identity


def _quota_exhausted_response(exc: QuotaExceededError) -> dict[str, object]:
    label = "text" if exc.quota_type == "text" else "image"
    return {
        "error": {
            "message": f"{label} quota exhausted",
            "type": "insufficient_quota",
            "code": "insufficient_quota",
        }
    }


def _user_key_concurrency_response(quota_type: QuotaType) -> dict[str, object]:
    return {
        "error": {
            "message": f"user key {quota_type} concurrency limit exceeded",
            "type": "rate_limit_error",
            "code": "user_key_concurrency_limit",
        }
    }


async def run_with_quota(
    call: Any,
    handler,
    *args,
    identity: dict[str, object],
    quota_type: QuotaType,
    requested_units: int,
    endpoint: str,
    sse: str = "openai",
    scheduler_runner=None,
    streaming: bool = False,
):
    lease = user_key_concurrency.try_acquire(identity, quota_type)
    if lease is None:
        raise HTTPException(
            status_code=429,
            detail=_user_key_concurrency_response(quota_type),
            headers={"Retry-After": "1"},
        )

    try:
        reservation = quota_service.reserve(
            identity,
            quota_type,
            requested_units,
            request_id=uuid.uuid4().hex,
            endpoint=endpoint,
        )
    except QuotaExceededError as exc:
        lease.release()
        raise HTTPException(status_code=429, detail=_quota_exhausted_response(exc)) from exc

    accounting = QuotaAccounting(reservation, quota_type, service=quota_service)

    def settle_success(result: object | None = None) -> None:
        try:
            accounting.commit_success(result)
        finally:
            lease.release()

    def settle_failure() -> None:
        try:
            accounting.release()
        finally:
            lease.release()

    try:
        return await call.run(
            handler,
            *args,
            sse=sse,
            scheduler_runner=scheduler_runner,
            streaming=streaming,
            on_success=settle_success,
            on_failure=settle_failure,
            on_stream_item=accounting.observe,
        )
    except BaseException:
        settle_failure()
        raise


def resolve_image_base_url(request: Request) -> str:
    return config.base_url or f"{request.url.scheme}://{request.headers.get('host', request.url.netloc)}"


def raise_image_quota_error(exc: Exception) -> None:
    message = str(exc)
    if "no available image quota" in message.lower():
        raise HTTPException(status_code=429, detail={"error": "no available image quota"}) from exc
    raise HTTPException(status_code=502, detail={"error": message}) from exc


def sanitize_cpa_pool(pool: dict | None) -> dict | None:
    if not isinstance(pool, dict):
        return None
    return {key: value for key, value in pool.items() if key != "secret_key"}


def sanitize_cpa_pools(pools: list[dict]) -> list[dict]:
    return [sanitized for pool in pools if (sanitized := sanitize_cpa_pool(pool)) is not None]


def sanitize_sub2api_server(server: dict | None) -> dict | None:
    if not isinstance(server, dict):
        return None
    sanitized = {key: value for key, value in server.items() if key not in {"password", "api_key"}}
    sanitized["has_api_key"] = bool(str(server.get("api_key") or "").strip())
    return sanitized


def sanitize_sub2api_servers(servers: list[dict]) -> list[dict]:
    return [sanitized for server in servers if (sanitized := sanitize_sub2api_server(server)) is not None]


def start_limited_account_watcher(stop_event: Event) -> Thread:
    interval_seconds = config.refresh_account_interval_minute * 60

    def worker() -> None:
        while not stop_event.is_set():
            try:
                account_service.cleanup_runtime_metadata()
                limited_tokens = account_service.list_limited_tokens()
                normal_tokens = account_service.list_normal_tokens()
                expiring_tokens = account_service.list_expiring_access_tokens()
                keepalive_tokens = account_service.list_refresh_token_keepalive_tokens()
                tokens = list(dict.fromkeys([*limited_tokens, *normal_tokens, *expiring_tokens]))
                expiring_token_set = set(expiring_tokens)
                keepalive_tokens = [token for token in keepalive_tokens if token not in expiring_token_set]
                if tokens:
                    account_service.refresh_accounts(tokens, quiet=True)
                if keepalive_tokens:
                    result = account_service.keepalive_refresh_tokens(keepalive_tokens)
                    if result.get("errors"):
                        logger.warning({"event": "account_watcher_keepalive_failed", "errors": result["errors"]})
            except Exception as exc:
                logger.error({"event": "account_watcher_failed", "error": str(exc)})
            stop_event.wait(interval_seconds)

    thread = Thread(target=worker, name="account-watcher", daemon=True)
    thread.start()
    return thread


def resolve_web_asset(requested_path: str) -> Path | None:
    if not WEB_DIST_DIR.exists():
        return None
    clean_path = requested_path.strip("/")
    base_dir = WEB_DIST_DIR.resolve()
    candidates = [base_dir / "index.html"] if not clean_path else [
        base_dir / Path(clean_path),
        base_dir / clean_path / "index.html",
        base_dir / f"{clean_path}.html",
    ]
    for candidate in candidates:
        try:
            candidate.resolve().relative_to(base_dir)
        except ValueError:
            continue
        if candidate.is_file():
            return candidate
    return None
