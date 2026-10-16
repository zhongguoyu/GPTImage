from __future__ import annotations

import asyncio
import io
import json
import re
import time
import uuid
import zipfile
from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, Header, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from pydantic import BaseModel, Field

from services.auth_service import auth_service

from api.support import (
    require_admin,
    sanitize_cpa_pool,
    sanitize_cpa_pools,
    sanitize_sub2api_server,
    sanitize_sub2api_servers,
)
from services.account_service import account_service
from services.config import config
from services.cpa_service import cpa_config, cpa_import_service, list_remote_files
from services.oauth_login_service import OAuthLoginError, oauth_login_service
from services.quota_service import QuotaKeyNotFoundError, QuotaLimitError, quota_service
from services.scheduler.service import scheduler_service
from services.sub2api_auto_sync_service import sub2api_auto_sync_service
from services.sub2api_service import (
    Sub2APISyncBusyError,
    list_remote_accounts as sub2api_list_remote_accounts,
    list_remote_groups as sub2api_list_remote_groups,
    sub2api_config,
    sub2api_import_service,
)
from utils.helper import anonymize_token



class UserKeyCreateRequest(BaseModel):
    name: str = ""
    text_unlimited: bool | None = None
    image_unlimited: bool | None = None
    text_limit: int = Field(default=0, ge=0)
    image_limit: int = Field(default=0, ge=0)
    text_concurrency: int | None = Field(default=None, ge=1)
    image_concurrency: int | None = Field(default=None, ge=1)


class UserKeyUpdateRequest(BaseModel):
    name: str | None = None
    enabled: bool | None = None
    web_login_enabled: bool | None = None
    key: str | None = None
    text_unlimited: bool | None = None
    image_unlimited: bool | None = None
    text_limit: int | None = Field(default=None, ge=0)
    image_limit: int | None = Field(default=None, ge=0)
    text_concurrency: int | None = Field(default=None, ge=1)
    image_concurrency: int | None = Field(default=None, ge=1)


class AccountCreateRequest(BaseModel):
    tokens: list[str] = Field(default_factory=list)
    accounts: list[dict[str, Any]] = Field(default_factory=list)


class AccountDeleteRequest(BaseModel):
    tokens: list[str] = Field(default_factory=list)


class AccountRefreshRequest(BaseModel):
    access_tokens: list[str] = Field(default_factory=list)


class AccountExportRequest(BaseModel):
    access_tokens: list[str] = Field(default_factory=list)
    format: Literal["json", "zip"] = "json"


class AccountUpdateRequest(BaseModel):
    access_token: str = ""
    type: str | None = None
    status: str | None = None
    quota: int | None = None
    proxy: str | None = None


class CPAPoolCreateRequest(BaseModel):
    name: str = ""
    base_url: str = ""
    secret_key: str = ""


class CPAPoolUpdateRequest(BaseModel):
    name: str | None = None
    base_url: str | None = None
    secret_key: str | None = None


class CPAImportRequest(BaseModel):
    names: list[str] = Field(default_factory=list)


class Sub2APIServerCreateRequest(BaseModel):
    name: str = ""
    base_url: str = ""
    email: str = ""
    password: str = ""
    api_key: str = ""
    group_id: str = ""
    auto_sync_interval_minutes: int = Field(default=0, ge=0)


class Sub2APIServerUpdateRequest(BaseModel):
    name: str | None = None
    base_url: str | None = None
    email: str | None = None
    password: str | None = None
    api_key: str | None = None
    group_id: str | None = None
    auto_sync_interval_minutes: int | None = Field(default=None, ge=0)


class Sub2APIImportRequest(BaseModel):
    account_ids: list[str] = Field(default_factory=list)


class OAuthLoginStartRequest(BaseModel):
    """起始 OAuth 桥。email_hint 可选，仅用于让 OpenAI 登录页预填邮箱。"""
    email_hint: str = ""


class OAuthLoginFinishRequest(BaseModel):
    """提交 callback。callback 既可以是完整 URL 也可以只填 code。"""
    session_id: str = ""
    callback: str = ""


def _account_payload_token(item: dict[str, Any]) -> str:
    return str(item.get("access_token") or item.get("accessToken") or "").strip()


def _unique_tokens(tokens: list[str]) -> list[str]:
    return list(dict.fromkeys(str(token or "").strip() for token in tokens if str(token or "").strip()))


def _download_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _safe_export_name(value: str, fallback: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return (clean or fallback)[:80]


def _list_user_keys_with_quota() -> list[dict[str, object]]:
    return quota_service.list_user_quotas(auth_service.list_keys(role="user"))


def _merge_user_key_quota(item: dict[str, object]) -> dict[str, object]:
    items = quota_service.list_user_quotas([item])
    return items[0] if items else dict(item)


def _account_zip_bytes(items: list[dict[str, str]]) -> bytes:
    buf = io.BytesIO()
    used_names: set[str] = set()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as archive:
        for index, item in enumerate(items, start=1):
            raw_name = item.get("email") or item.get("account_id") or f"account-{index:03d}"
            base_name = _safe_export_name(raw_name, f"account-{index:03d}")
            name = base_name
            suffix = 2
            while name in used_names:
                name = f"{base_name}-{suffix}"
                suffix += 1
            used_names.add(name)
            archive.writestr(
                f"{name}.json",
                json.dumps(item, ensure_ascii=False, indent=2) + "\n",
            )
    return buf.getvalue()


_IMAGE_BREAKER_LANES = {"image_generation", "image_edit"}


def _attach_image_rate_limit(items: list[dict[str, Any]]) -> None:
    """为账号列表附加图片滑动窗口限流冷却剩余秒数（内存态，不落盘）。

    两个来源取最大值：legacy 路径的 account_service 内存表，以及调度器
    breaker（scheduler 路径的 open_breaker 冷却）。breaker 的 key 已被
    anonymize（sha256 确定性哈希），与列表 token 重新 anonymize 后即可对上。
    """
    if not items:
        return
    now = time.time()
    legacy = account_service.image_rate_limit_snapshot()
    breaker_remaining: dict[str, float] = {}
    try:
        for health in scheduler_service.account_pool.snapshot_health():
            if str(health.get("lane") or "") not in _IMAGE_BREAKER_LANES:
                continue
            if str(health.get("breaker_state") or "") != "open":
                continue
            remaining = max(0.0, float(health.get("cooldown_until") or 0.0) - now)
            if remaining <= 0:
                continue
            account_ref = str(health.get("account_key") or "")
            if account_ref:
                breaker_remaining[account_ref] = max(breaker_remaining.get(account_ref, 0.0), remaining)
    except Exception:
        breaker_remaining = {}
    if not legacy and not breaker_remaining:
        return
    for item in items:
        if item.get("account_info_stale") or item.get("account_info_mode") == "sub2api":
            continue
        token = str(item.get("access_token") or "").strip()
        if not token:
            continue
        remaining = max(
            float(legacy.get(token, 0.0)),
            float(breaker_remaining.get(anonymize_token(token), 0.0)),
        )
        if remaining > 0:
            item["image_rate_limit_remaining_secs"] = int(remaining + 0.999)


def create_router() -> APIRouter:
    router = APIRouter()

    @router.get("/api/auth/users")
    async def list_user_keys(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return {"items": _list_user_keys_with_quota()}

    @router.post("/api/auth/users")
    async def create_user_key(body: UserKeyCreateRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        try:
            item, raw_key = auth_service.create_key(role="user", name=body.name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
        try:
            quota = quota_service.set_limits(
                str(item.get("id") or ""),
                text_unlimited=body.text_unlimited if body.text_unlimited is not None else "text_limit" not in body.model_fields_set,
                image_unlimited=body.image_unlimited if body.image_unlimited is not None else "image_limit" not in body.model_fields_set,
                text_limit=body.text_limit,
                image_limit=body.image_limit,
                text_concurrency=body.text_concurrency or config.user_key_text_concurrency,
                image_concurrency=body.image_concurrency or config.user_key_image_concurrency,
            )
        except Exception as exc:
            try:
                auth_service.delete_key(str(item.get("id") or ""), role="user")
            except Exception:
                pass
            raise HTTPException(status_code=500, detail={"error": "failed to initialize user key quota"}) from exc
        item = {**item, **{key: value for key, value in quota.items() if key != "key_id"}}
        return {"item": item, "key": raw_key, "items": _list_user_keys_with_quota()}

    @router.post("/api/auth/users/{key_id}")
    async def update_user_key(
            key_id: str,
            body: UserKeyUpdateRequest,
            authorization: str | None = Header(default=None),
    ):
        require_admin(authorization)
        auth_updates = {
            key: value
            for key, value in {
                "name": body.name,
                "enabled": body.enabled,
                "web_login_enabled": body.web_login_enabled,
                "key": body.key,
            }.items()
            if value is not None
        }
        has_quota_updates = any(
            value is not None
            for value in (
                body.text_unlimited,
                body.image_unlimited,
                body.text_limit,
                body.image_limit,
                body.text_concurrency,
                body.image_concurrency,
            )
        )
        if not auth_updates and not has_quota_updates:
            raise HTTPException(status_code=400, detail={"error": "还没有检测到改动，请修改后再保存"})

        existing_item = next(
            (candidate for candidate in auth_service.list_keys(role="user") if candidate.get("id") == key_id),
            None,
        )
        if existing_item is None:
            raise HTTPException(status_code=404, detail={"error": "这条用户密钥不存在，可能已经被删除"})

        quota: dict[str, object] | None = None
        if has_quota_updates:
            try:
                quota = quota_service.set_limits(
                    key_id,
                    text_unlimited=body.text_unlimited,
                    image_unlimited=body.image_unlimited,
                    text_limit=body.text_limit,
                    image_limit=body.image_limit,
                    text_concurrency=body.text_concurrency,
                    image_concurrency=body.image_concurrency,
                )
            except QuotaLimitError as exc:
                raise HTTPException(
                    status_code=409,
                    detail={"error": str(exc), "code": exc.code},
                ) from exc
            except QuotaKeyNotFoundError as exc:
                raise HTTPException(status_code=404, detail={"error": "这条用户密钥不存在，可能已经被删除"}) from exc
        try:
            item = auth_service.update_key(key_id, auth_updates, role="user") if auth_updates else None
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
        if auth_updates and item is None:
            raise HTTPException(status_code=404, detail={"error": "这条用户密钥不存在，可能已经被删除"})
        if item is None:
            item = existing_item
        if quota is None:
            item = _merge_user_key_quota(item)
        else:
            item = {**item, **{key: value for key, value in quota.items() if key != "key_id"}}
        return {"item": item, "items": _list_user_keys_with_quota()}

    @router.delete("/api/auth/users/{key_id}")
    async def delete_user_key(key_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        if not auth_service.delete_key(key_id, role="user"):
            raise HTTPException(status_code=404, detail={"error": "这条用户密钥不存在，可能已经被删除"})
        quota_service.mark_deleted(key_id)
        return {"items": _list_user_keys_with_quota()}

    @router.get("/api/accounts")
    async def get_accounts(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        items = account_service.list_accounts()
        _attach_image_rate_limit(items)
        return {"items": items}

    @router.post("/api/accounts")
    async def create_accounts(body: AccountCreateRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        account_payloads = [item for item in body.accounts if isinstance(item, dict)]
        payload_tokens = [_account_payload_token(item) for item in account_payloads]
        tokens = _unique_tokens([*body.tokens, *payload_tokens])
        if not tokens:
            raise HTTPException(status_code=400, detail={"error": "tokens is required"})
        if account_payloads:
            result = account_service.add_account_items(account_payloads)
            payload_token_set = set(_unique_tokens(payload_tokens))
            extra_tokens = [token for token in tokens if token not in payload_token_set]
            if extra_tokens:
                extra_result = account_service.add_accounts(extra_tokens)
                result["added"] = int(result.get("added") or 0) + int(extra_result.get("added") or 0)
                result["skipped"] = int(result.get("skipped") or 0) + int(extra_result.get("skipped") or 0)
        else:
            result = account_service.add_accounts(tokens)
        refresh_result = account_service.refresh_accounts(tokens, quiet=True)
        return {
            **result,
            "refreshed": refresh_result.get("refreshed", 0),
            "errors": refresh_result.get("errors", []),
            "items": refresh_result.get("items", result.get("items", [])),
        }

    @router.delete("/api/accounts")
    async def delete_accounts(body: AccountDeleteRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        tokens = [str(token or "").strip() for token in body.tokens if str(token or "").strip()]
        if not tokens:
            raise HTTPException(status_code=400, detail={"error": "tokens is required"})
        return account_service.delete_accounts(tokens)

    @router.post("/api/accounts/refresh")
    async def refresh_accounts(body: AccountRefreshRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        access_tokens = [str(token or "").strip() for token in body.access_tokens if str(token or "").strip()]
        if not access_tokens:
            access_tokens = account_service.list_tokens()
        if not access_tokens:
            raise HTTPException(status_code=400, detail={"error": "access_tokens is required"})

        progress_id = str(uuid.uuid4())

        async def _do_refresh():
            try:
                await run_in_threadpool(
                    account_service.refresh_accounts,
                    access_tokens,
                    progress_id,
                    False,
                    quiet=True,
                    log_summary=True,
                )
            except Exception as e:
                account_service.finish_refresh_progress(progress_id, error=str(e))

        asyncio.create_task(_do_refresh())

        return {"progress_id": progress_id}

    @router.get("/api/accounts/refresh/progress/{progress_id}")
    async def get_refresh_progress(progress_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        progress = account_service.get_refresh_progress(progress_id)
        if progress is None:
            raise HTTPException(status_code=404, detail={"error": "progress not found"})
        return progress

    @router.post("/api/accounts/re-login")
    async def re_login_accounts(body: AccountRefreshRequest, authorization: str | None = Header(default=None)):
        """对选中账号执行密码重新登录流程（密码登录→验证码登录→刷新token）。"""
        require_admin(authorization)
        access_tokens = [str(token or "").strip() for token in body.access_tokens if str(token or "").strip()]
        if not access_tokens:
            raise HTTPException(status_code=400, detail={"error": "access_tokens is required"})

        progress_id = str(uuid.uuid4())

        async def _do_relogin():
            try:
                await run_in_threadpool(account_service.re_login_accounts, access_tokens, progress_id)
            except Exception as e:
                account_service.finish_relogin_progress(progress_id, error=str(e))

        asyncio.create_task(_do_relogin())

        return {"progress_id": progress_id}

    @router.get("/api/accounts/re-login/progress/{progress_id}")
    async def get_relogin_progress(progress_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        progress = account_service.get_relogin_progress(progress_id)
        if progress is None:
            raise HTTPException(status_code=404, detail={"error": "progress not found"})
        return progress

    @router.post("/api/accounts/export")
    async def export_accounts(body: AccountExportRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        access_tokens = _unique_tokens(body.access_tokens)
        items = account_service.build_export_items(access_tokens)
        if not items:
            raise HTTPException(
                status_code=400,
                detail={"error": "没有可导出的完整账号，需要同时有 access_token、refresh_token 和 id_token"},
            )

        timestamp = _download_timestamp()
        if body.format == "zip":
            content = _account_zip_bytes(items)
            return Response(
                content,
                media_type="application/zip",
                headers={"Content-Disposition": f'attachment; filename="codex-accounts-{timestamp}.zip"'},
            )

        payload: dict[str, str] | list[dict[str, str]] = items[0] if len(items) == 1 else items
        return Response(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="codex-accounts-{timestamp}.json"'},
        )

    @router.post("/api/accounts/update")
    async def update_account(body: AccountUpdateRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        access_token = str(body.access_token or "").strip()
        if not access_token:
            raise HTTPException(status_code=400, detail={"error": "access_token is required"})
        updates = {key: value for key, value in {"type": body.type, "status": body.status, "quota": body.quota, "proxy": body.proxy}.items() if value is not None}
        if not updates:
            raise HTTPException(status_code=400, detail={"error": "还没有检测到改动，请修改后再保存"})
        account = account_service.update_account(access_token, updates)
        if account is None:
            raise HTTPException(status_code=404, detail={"error": "account not found"})
        return {"item": account, "items": account_service.list_accounts()}

    @router.post("/api/accounts/oauth/start")
    async def start_oauth_login(
            body: OAuthLoginStartRequest,
            authorization: str | None = Header(default=None),
    ):
        """登记一次 PKCE 会话，返回可让用户浏览器打开的 authorize URL。"""
        require_admin(authorization)
        try:
            return await run_in_threadpool(oauth_login_service.start, body.email_hint)
        except OAuthLoginError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.post("/api/accounts/oauth/finish")
    async def finish_oauth_login(
            body: OAuthLoginFinishRequest,
            authorization: str | None = Header(default=None),
    ):
        """收用户从浏览器抓回的 callback URL / code，换出 token 三件套并落盘。"""
        require_admin(authorization)
        # 入参日志：截断敏感字段，仅保留前几位，方便排错而不泄密
        cb_preview = (body.callback or "")[:80]
        sid_preview = (body.session_id or "")[:8]
        print(
            f"[oauth-login] finish called: session_id={sid_preview}..., callback_preview={cb_preview!r}",
            flush=True,
        )
        try:
            tokens = await run_in_threadpool(oauth_login_service.finish, body.session_id, body.callback)
        except OAuthLoginError as exc:
            print(f"[oauth-login] finish rejected: {exc}", flush=True)
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

        payload = {
            "access_token": tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "id_token": tokens["id_token"],
            "source_type": "oauth_login",
        }
        add_result = await run_in_threadpool(account_service.add_account_items, [payload])
        refresh_result = await run_in_threadpool(
            account_service.refresh_accounts, [tokens["access_token"]]
        )
        return {
            **add_result,
            "refreshed": refresh_result.get("refreshed", 0),
            "errors": refresh_result.get("errors", []),
            "items": refresh_result.get("items", add_result.get("items", [])),
        }

    @router.get("/api/cpa/pools")
    async def list_cpa_pools(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return {"pools": sanitize_cpa_pools(cpa_config.list_pools())}

    @router.post("/api/cpa/pools")
    async def create_cpa_pool(body: CPAPoolCreateRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        if not body.base_url.strip():
            raise HTTPException(status_code=400, detail={"error": "base_url is required"})
        if not body.secret_key.strip():
            raise HTTPException(status_code=400, detail={"error": "secret_key is required"})
        pool = cpa_config.add_pool(name=body.name, base_url=body.base_url, secret_key=body.secret_key)
        return {"pool": sanitize_cpa_pool(pool), "pools": sanitize_cpa_pools(cpa_config.list_pools())}

    @router.post("/api/cpa/pools/{pool_id}")
    async def update_cpa_pool(pool_id: str, body: CPAPoolUpdateRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        pool = cpa_config.update_pool(pool_id, body.model_dump(exclude_none=True))
        if pool is None:
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        return {"pool": sanitize_cpa_pool(pool), "pools": sanitize_cpa_pools(cpa_config.list_pools())}

    @router.delete("/api/cpa/pools/{pool_id}")
    async def delete_cpa_pool(pool_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        if not cpa_config.delete_pool(pool_id):
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        return {"pools": sanitize_cpa_pools(cpa_config.list_pools())}

    @router.get("/api/cpa/pools/{pool_id}/files")
    async def cpa_pool_files(pool_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        pool = cpa_config.get_pool(pool_id)
        if pool is None:
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        return {"pool_id": pool_id, "files": await run_in_threadpool(list_remote_files, pool)}

    @router.post("/api/cpa/pools/{pool_id}/import")
    async def cpa_pool_import(pool_id: str, body: CPAImportRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        pool = cpa_config.get_pool(pool_id)
        if pool is None:
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        try:
            job = cpa_import_service.start_import(pool, body.names)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
        return {"import_job": job}

    @router.get("/api/cpa/pools/{pool_id}/import")
    async def cpa_pool_import_progress(pool_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        pool = cpa_config.get_pool(pool_id)
        if pool is None:
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        return {"import_job": pool.get("import_job")}

    @router.get("/api/sub2api/servers")
    async def list_sub2api_servers(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return {"servers": sanitize_sub2api_servers(sub2api_config.list_servers())}

    @router.post("/api/sub2api/servers")
    async def create_sub2api_server(body: Sub2APIServerCreateRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        if not body.base_url.strip():
            raise HTTPException(status_code=400, detail={"error": "base_url is required"})
        has_login = body.email.strip() and body.password.strip()
        has_api_key = bool(body.api_key.strip())
        if not has_login and not has_api_key:
            raise HTTPException(status_code=400, detail={"error": "email+password or api_key is required"})
        server = sub2api_config.add_server(
            name=body.name,
            base_url=body.base_url,
            email=body.email,
            password=body.password,
            api_key=body.api_key,
            group_id=body.group_id,
            auto_sync_interval_minutes=body.auto_sync_interval_minutes,
        )
        if body.auto_sync_interval_minutes > 0:
            sub2api_auto_sync_service.notify_server_changed(server["id"], immediate=True)
        return {"server": sanitize_sub2api_server(server), "servers": sanitize_sub2api_servers(sub2api_config.list_servers())}

    @router.post("/api/sub2api/servers/{server_id}")
    async def update_sub2api_server(server_id: str, body: Sub2APIServerUpdateRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        updates = body.model_dump(exclude_none=True)
        sync_fields = {"base_url", "email", "password", "api_key", "group_id", "auto_sync_interval_minutes"}
        sync_changed = bool(sync_fields.intersection(updates))
        server = sub2api_config.update_server(server_id, updates)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        if sync_changed:
            sub2api_auto_sync_service.notify_server_changed(
                server_id,
                immediate=int(server.get("auto_sync_interval_minutes") or 0) > 0,
            )
        return {"server": sanitize_sub2api_server(server), "servers": sanitize_sub2api_servers(sub2api_config.list_servers())}

    @router.delete("/api/sub2api/servers/{server_id}")
    async def delete_sub2api_server(server_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        if not sub2api_config.delete_server(server_id):
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        sub2api_auto_sync_service.server_deleted(server_id)
        return {"servers": sanitize_sub2api_servers(sub2api_config.list_servers())}

    @router.get("/api/sub2api/servers/{server_id}/groups")
    async def sub2api_server_groups(server_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        server = sub2api_config.get_server(server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        try:
            groups = await run_in_threadpool(sub2api_list_remote_groups, server)
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": str(exc)}) from exc
        return {"server_id": server_id, "groups": groups}

    @router.get("/api/sub2api/servers/{server_id}/accounts")
    async def sub2api_server_accounts(server_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        server = sub2api_config.get_server(server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        try:
            accounts = await run_in_threadpool(sub2api_list_remote_accounts, server)
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": str(exc)}) from exc
        return {"server_id": server_id, "accounts": accounts}

    @router.post("/api/sub2api/servers/{server_id}/import")
    async def sub2api_server_import(server_id: str, body: Sub2APIImportRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        server = sub2api_config.get_server(server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        try:
            job = sub2api_import_service.start_import(server, body.account_ids)
        except Sub2APISyncBusyError as exc:
            raise HTTPException(
                status_code=409,
                detail={"error": str(exc), "code": Sub2APISyncBusyError.code},
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
        return {"import_job": job}

    @router.get("/api/sub2api/servers/{server_id}/import")
    async def sub2api_server_import_progress(server_id: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        server = sub2api_config.get_server(server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        return {"import_job": server.get("import_job")}

    return router
