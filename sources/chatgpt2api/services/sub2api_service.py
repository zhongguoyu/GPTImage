"""Sub2API integration for browsing and importing ChatGPT OAuth accounts from a sub2api admin."""

from __future__ import annotations

import json
import copy
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from curl_cffi.requests import Session

from services.account_service import account_service
from services.config import DATA_DIR


SUB2API_CONFIG_FILE = DATA_DIR / "sub2api_config.json"

# Cached JWT per server to avoid re-login on every list/import call.
# Token lifetime on sub2api defaults to 24h; we refresh 5 min before expiry.
_TOKEN_REFRESH_SKEW = 5 * 60
AUTO_SYNC_STATUSES = {"disabled", "idle", "running", "completed", "failed"}
MAX_IMPORT_ERRORS = 50


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: object) -> str:
    return str(value or "").strip()


def _normalize_interval(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value if value > 0 else 0
    if isinstance(value, str):
        clean = value.strip()
        if clean.isdecimal():
            interval = int(clean)
            return interval if interval > 0 else 0
    return 0


def _nonnegative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _normalize_import_job(raw: object, *, fail_unfinished: bool) -> dict | None:
    if not isinstance(raw, dict):
        return None
    status = _clean(raw.get("status")) or "failed"
    if fail_unfinished and status in {"pending", "running"}:
        status = "failed"
    return {
        "job_id": _clean(raw.get("job_id")) or uuid.uuid4().hex,
        "status": status,
        "created_at": _clean(raw.get("created_at")) or _now_iso(),
        "updated_at": _clean(raw.get("updated_at")) or _clean(raw.get("created_at")) or _now_iso(),
        "total": _nonnegative_int(raw.get("total")),
        "completed": _nonnegative_int(raw.get("completed")),
        "added": _nonnegative_int(raw.get("added")),
        "skipped": _nonnegative_int(raw.get("skipped")),
        "refreshed": _nonnegative_int(raw.get("refreshed")),
        "failed": _nonnegative_int(raw.get("failed")),
        "errors": raw.get("errors") if isinstance(raw.get("errors"), list) else [],
    }


def _normalize_auto_sync(raw: object, *, interval: int) -> dict:
    item = raw if isinstance(raw, dict) else {}
    status = _clean(item.get("status")) or ("idle" if interval > 0 else "disabled")
    if status not in AUTO_SYNC_STATUSES:
        status = "idle" if interval > 0 else "disabled"
    if interval <= 0:
        status = "disabled"
    return {
        "status": status,
        "last_started_at": _clean(item.get("last_started_at")) or None,
        "last_finished_at": _clean(item.get("last_finished_at")) or None,
        "last_success_at": _clean(item.get("last_success_at")) or None,
        "next_run_at": _clean(item.get("next_run_at")) or None,
        "last_error": _clean(item.get("last_error")),
        "last_remote_count": _nonnegative_int(item.get("last_remote_count")),
        "last_import_job_id": _clean(item.get("last_import_job_id")),
        "last_import_failed": _nonnegative_int(item.get("last_import_failed")),
    }


def _normalize_server(raw: dict) -> dict:
    interval = _normalize_interval(raw.get("auto_sync_interval_minutes"))
    return {
        "id": _clean(raw.get("id")) or _new_id(),
        "name": _clean(raw.get("name")),
        "base_url": _clean(raw.get("base_url")),
        "email": _clean(raw.get("email")),
        "password": _clean(raw.get("password")),
        "api_key": _clean(raw.get("api_key")),
        "group_id": _clean(raw.get("group_id")),
        "auto_sync_interval_minutes": interval,
        "auto_sync": _normalize_auto_sync(raw.get("auto_sync"), interval=interval),
        "import_job": _normalize_import_job(raw.get("import_job"), fail_unfinished=True),
    }


class Sub2APIConfig:
    def __init__(self, store_file: Path):
        self._store_file = store_file
        self._lock = Lock()
        self._servers: list[dict] = self._load()

    def _load(self) -> list[dict]:
        if not self._store_file.exists():
            return []
        try:
            raw = json.loads(self._store_file.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                return [_normalize_server(item) for item in raw if isinstance(item, dict)]
        except Exception:
            pass
        return []

    def _save(self) -> None:
        self._store_file.parent.mkdir(parents=True, exist_ok=True)
        self._store_file.write_text(
            json.dumps(self._servers, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def list_servers(self) -> list[dict]:
        with self._lock:
            return copy.deepcopy(self._servers)

    def get_server(self, server_id: str) -> dict | None:
        with self._lock:
            for server in self._servers:
                if server["id"] == server_id:
                    return copy.deepcopy(server)
        return None

    def add_server(
        self,
        *,
        name: str,
        base_url: str,
        email: str,
        password: str,
        api_key: str,
        group_id: str = "",
        auto_sync_interval_minutes: int = 0,
    ) -> dict:
        server = _normalize_server({
            "id": _new_id(),
            "name": name,
            "base_url": base_url,
            "email": email,
            "password": password,
            "api_key": api_key,
            "group_id": group_id,
            "auto_sync_interval_minutes": auto_sync_interval_minutes,
        })
        with self._lock:
            self._servers.append(server)
            self._save()
        _token_cache.pop(server["id"], None)
        return copy.deepcopy(server)

    def update_server(self, server_id: str, updates: dict) -> dict | None:
        with self._lock:
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                merged = {**server, **{k: v for k, v in updates.items() if v is not None}, "id": server_id}
                self._servers[index] = _normalize_server(merged)
                self._save()
                result = copy.deepcopy(self._servers[index])
                break
            else:
                return None
        _token_cache.pop(server_id, None)
        return result

    def set_auto_sync_state(self, server_id: str, **updates: object) -> dict | None:
        with self._lock:
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                next_server = copy.deepcopy(server)
                auto_sync = dict(next_server.get("auto_sync") or {})
                auto_sync.update(updates)
                next_server["auto_sync"] = auto_sync
                normalized = _normalize_server(next_server)
                self._servers[index] = normalized
                self._save()
                return copy.deepcopy(normalized)
        return None

    def delete_server(self, server_id: str) -> bool:
        with self._lock:
            before = len(self._servers)
            self._servers = [server for server in self._servers if server["id"] != server_id]
            removed = len(self._servers) < before
            if removed:
                self._save()
        if removed:
            _token_cache.pop(server_id, None)
        return removed

    def set_import_job(self, server_id: str, import_job: dict | None) -> dict | None:
        with self._lock:
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                next_server = dict(server)
                next_server["import_job"] = _normalize_import_job(import_job, fail_unfinished=False)
                self._servers[index] = next_server
                self._save()
                return copy.deepcopy(next_server)
        return None

    def get_import_job(self, server_id: str) -> dict | None:
        with self._lock:
            for server in self._servers:
                if server["id"] == server_id:
                    job = server.get("import_job")
                    return copy.deepcopy(job) if isinstance(job, dict) else None
        return None


# Per-server cached access token: {server_id: (jwt, expires_at_epoch)}
_token_cache: dict[str, tuple[str, float]] = {}
_token_cache_lock = Lock()


def _login(base_url: str, email: str, password: str) -> tuple[str, float]:
    url = f"{base_url.rstrip('/')}/api/v1/auth/login"
    session = Session(verify=True)
    try:
        response = session.post(
            url,
            json={"email": email, "password": password},
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"sub2api login failed: HTTP {response.status_code} {response.text[:200]}")
        payload = response.json()
    finally:
        session.close()

    body = _unwrap_envelope(payload)
    if not isinstance(body, dict):
        raise RuntimeError("sub2api login payload is invalid")

    token = _clean(body.get("access_token"))
    if not token:
        raise RuntimeError("sub2api login did not return access_token")

    expires_in = int(body.get("expires_in") or 3600)
    expires_at = time.time() + max(60, expires_in) - _TOKEN_REFRESH_SKEW
    return token, expires_at


def _auth_headers(server: dict) -> dict[str, str]:
    api_key = _clean(server.get("api_key"))
    if api_key:
        return {"x-api-key": api_key, "Accept": "application/json"}

    email = _clean(server.get("email"))
    password = _clean(server.get("password"))
    if not email or not password:
        raise RuntimeError("sub2api server requires email+password or api_key")

    server_id = _clean(server.get("id"))
    base_url = _clean(server.get("base_url"))

    with _token_cache_lock:
        cached = _token_cache.get(server_id)
        if cached and cached[1] > time.time():
            return {"Authorization": f"Bearer {cached[0]}", "Accept": "application/json"}

    token, expires_at = _login(base_url, email, password)
    with _token_cache_lock:
        _token_cache[server_id] = (token, expires_at)
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _extract_access_token(credentials: object) -> str:
    if not isinstance(credentials, dict):
        return ""
    for key in ("access_token", "accessToken", "token"):
        value = _clean(credentials.get(key))
        if value:
            return value
    return ""


def _unwrap_envelope(payload: object) -> object:
    """Peel sub2api's `{code, message, data}` envelope, returning the inner `data` field
    when present. Also handles unwrapped responses from older/alt versions."""
    if isinstance(payload, dict) and "data" in payload and "code" in payload:
        return payload.get("data")
    return payload


def _extract_paged_items(payload: object) -> tuple[list, int]:
    """Return (items, total) from a paginated sub2api response.

    Handles both the wrapped shape `{code,data:{items,total,...}}` and a few looser
    variants (`{data:[...]}`, `[...]`, `{items:[...],total:N}`)."""
    inner = _unwrap_envelope(payload)
    if isinstance(inner, list):
        return inner, len(inner)
    if isinstance(inner, dict):
        for key in ("items", "data", "list"):
            value = inner.get(key)
            if isinstance(value, list):
                return value, int(inner.get("total") or len(value))
    return [], 0


def list_remote_accounts(server: dict) -> list[dict]:
    """Return a flat list of OpenAI OAuth accounts from a sub2api server."""
    base_url = _clean(server.get("base_url"))
    if not base_url:
        return []

    headers = _auth_headers(server)
    group_id = _clean(server.get("group_id"))

    session = Session(verify=True)
    items: list[dict] = []
    try:
        page = 1
        while True:
            params: dict[str, object] = {
                "platform": "openai",
                "type": "oauth",
                "page": page,
                "page_size": 200,
            }
            if group_id:
                params["group"] = group_id
            response = session.get(
                f"{base_url.rstrip('/')}/api/v1/admin/accounts",
                headers=headers,
                params=params,
                timeout=30,
            )
            if not response.ok:
                raise RuntimeError(f"sub2api list failed: HTTP {response.status_code} {response.text[:200]}")
            payload = response.json()

            data, total = _extract_paged_items(payload)
            if not data:
                break

            for account in data:
                if not isinstance(account, dict):
                    continue
                credentials = account.get("credentials") if isinstance(account.get("credentials"), dict) else {}
                account_id = account.get("id")
                if account_id is None:
                    continue
                items.append({
                    "id": str(account_id),
                    "name": _clean(account.get("name")),
                    "email": _clean(credentials.get("email")) or _clean(account.get("name")),
                    "plan_type": _clean(credentials.get("plan_type")),
                    "status": _clean(account.get("status")),
                    "expires_at": _clean(credentials.get("expires_at")),
                    "has_refresh_token": bool(_clean(credentials.get("refresh_token"))),
                })

            if page * 200 >= total or len(data) < 200:
                break
            page += 1
    finally:
        session.close()

    return items


def list_remote_groups(server: dict) -> list[dict]:
    """Return OpenAI account groups from a sub2api server."""
    base_url = _clean(server.get("base_url"))
    if not base_url:
        return []

    headers = _auth_headers(server)

    session = Session(verify=True)
    items: list[dict] = []
    try:
        page = 1
        while True:
            response = session.get(
                f"{base_url.rstrip('/')}/api/v1/admin/groups",
                headers=headers,
                params={
                    "page": page,
                    "page_size": 200,
                },
                timeout=30,
            )
            if not response.ok:
                raise RuntimeError(f"sub2api groups failed: HTTP {response.status_code} {response.text[:200]}")
            payload = response.json()

            data, total = _extract_paged_items(payload)
            if not data:
                break

            for group in data:
                if not isinstance(group, dict):
                    continue
                group_id = group.get("id")
                if group_id is None:
                    continue
                items.append({
                    "id": str(group_id),
                    "name": _clean(group.get("name")),
                    "description": _clean(group.get("description")),
                    "platform": _clean(group.get("platform")),
                    "status": _clean(group.get("status")),
                    "account_count": int(group.get("account_count") or 0),
                    "active_account_count": int(group.get("active_account_count") or 0),
                })

            if page * 200 >= total or len(data) < 200:
                break
            page += 1
    finally:
        session.close()

    return items


SUB2API_EXPORT_BATCH_SIZE = 100


def _fetch_access_tokens_for_accounts(server: dict, account_ids: list[str]) -> tuple[list[str], list[dict]]:
    """Return exported access tokens and per-account errors from sub2api."""
    base_url = _clean(server.get("base_url"))
    headers = _auth_headers(server)
    ids = [_clean(item) for item in account_ids if _clean(item)]
    if not ids:
        return [], []

    tokens: list[str] = []
    errors: list[dict] = []

    for offset in range(0, len(ids), SUB2API_EXPORT_BATCH_SIZE):
        batch_ids = ids[offset:offset + SUB2API_EXPORT_BATCH_SIZE]
        session = Session(verify=True)
        try:
            response = session.get(
                f"{base_url.rstrip('/')}/api/v1/admin/accounts/data",
                headers=headers,
                params={"ids": ",".join(batch_ids), "timezone": "Asia/Shanghai"},
                timeout=30,
            )
            if not response.ok:
                raise RuntimeError(f"HTTP {response.status_code}")
            payload = response.json()
        finally:
            session.close()

        data = _unwrap_envelope(payload)
        accounts = data.get("accounts") if isinstance(data, dict) else None
        if not isinstance(accounts, list):
            raise RuntimeError("invalid export payload")

        for account in accounts:
            if not isinstance(account, dict):
                continue
            credentials = account.get("credentials") if isinstance(account.get("credentials"), dict) else {}
            token = _extract_access_token(credentials)
            account_id = _clean(account.get("id")) or _clean(credentials.get("chatgpt_account_id")) or _clean(account.get("name"))
            if token:
                tokens.append(token)
            else:
                errors.append({"name": account_id or _clean(account.get("name")) or "unknown", "error": "missing access_token"})

        if len(accounts) < len(batch_ids):
            errors.append({"name": ",".join(batch_ids), "error": f"exported {len(accounts)}/{len(batch_ids)} accounts"})

    return tokens, errors


class Sub2APISyncBusyError(RuntimeError):
    code = "sub2api_sync_busy"


class _OperationLease:
    def __init__(self, owner: "Sub2APIImportService", server_id: str, lock: Lock):
        self._owner = owner
        self._server_id = server_id
        self._lock = lock
        self._released = False

    def __enter__(self) -> "_OperationLease":
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.release()

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._owner._release_operation(self._server_id, self._lock)


class Sub2APIImportService:
    def __init__(self, sub2api_config: Sub2APIConfig):
        self._config = sub2api_config
        self._locks: dict[str, Lock] = {}
        self._locks_lock = Lock()
        self._job_events: dict[str, threading.Event] = {}
        self._job_events_lock = Lock()

    def _acquire_operation(self, server_id: str) -> _OperationLease:
        with self._locks_lock:
            operation_lock = self._locks.setdefault(server_id, Lock())
            if not operation_lock.acquire(blocking=False):
                raise Sub2APISyncBusyError("该连接已有同步任务正在运行")
            return _OperationLease(self, server_id, operation_lock)

    @contextmanager
    def acquire_operation(self, server_id: str):
        lease = self._acquire_operation(server_id)
        try:
            yield lease
        finally:
            lease.release()

    def _release_operation(self, server_id: str, operation_lock: Lock) -> None:
        with self._locks_lock:
            operation_lock.release()
            if self._locks.get(server_id) is operation_lock:
                self._locks.pop(server_id, None)

    def _get_job_event(self, job_id: str) -> threading.Event:
        with self._job_events_lock:
            return self._job_events.setdefault(job_id, threading.Event())

    def start_import(
        self,
        server: dict,
        account_ids: list[str],
        *,
        operation_lock_held: bool = False,
    ) -> dict:
        ids = [_clean(item) for item in account_ids if _clean(item)]
        if not ids:
            raise ValueError("account ids is required")

        server_id = _clean(server.get("id"))
        operation_lease = None if operation_lock_held else self._acquire_operation(server_id)
        job = {
            "job_id": uuid.uuid4().hex,
            "status": "pending",
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "total": len(ids),
            "completed": 0,
            "added": 0,
            "skipped": 0,
            "refreshed": 0,
            "failed": 0,
            "errors": [],
        }
        try:
            saved = self._config.set_import_job(server_id, job)
            if saved is None:
                raise ValueError("server not found")
            self._get_job_event(job["job_id"])
            thread = threading.Thread(
                target=self._run_import,
                args=(server_id, server, ids, operation_lease),
                name=f"sub2api-import-{server_id}",
                daemon=True,
            )
            thread.start()
            operation_lease = None
            return copy.deepcopy(saved.get("import_job") or job)
        except BaseException:
            if operation_lease is not None:
                operation_lease.release()
            raise

    def _update_job(self, server_id: str, **updates) -> None:
        current = self._config.get_import_job(server_id)
        if current is None:
            return
        next_job = {**current, **updates, "updated_at": _now_iso()}
        self._config.set_import_job(server_id, next_job)
        if next_job.get("status") in {"completed", "failed"}:
            with self._job_events_lock:
                event = self._job_events.pop(next_job["job_id"], None)
            if event is not None:
                event.set()

    def wait_for_job(
        self,
        server_id: str,
        job_id: str,
        *,
        timeout: float | None = None,
        stop_event: threading.Event | None = None,
    ) -> dict:
        deadline = time.monotonic() + timeout if timeout is not None else None
        event = self._get_job_event(job_id)
        while True:
            job = self._config.get_import_job(server_id)
            if job is None:
                raise ValueError("server not found")
            if job.get("job_id") != job_id:
                raise ValueError("import job was replaced")
            if job.get("status") in {"completed", "failed"}:
                with self._job_events_lock:
                    self._job_events.pop(job_id, None)
                return job
            if stop_event is not None and stop_event.is_set():
                raise RuntimeError("import wait stopped")
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for import job")
            wait_for = 0.25
            if deadline is not None:
                wait_for = max(0.01, min(wait_for, deadline - time.monotonic()))
            event.wait(wait_for)
            event.clear()

    def _append_error(self, server_id: str, account_id: str, message: str) -> None:
        current = self._config.get_import_job(server_id)
        if current is None:
            return
        errors = list(current.get("errors") or [])
        errors.append({"name": account_id, "error": message})
        self._update_job(
            server_id,
            errors=errors[-MAX_IMPORT_ERRORS:],
            failed=_nonnegative_int(current.get("failed")) + 1,
        )

    def _run_import(
        self,
        server_id: str,
        server: dict,
        account_ids: list[str],
        operation_lease: _OperationLease | None,
    ) -> None:
        try:
            self._update_job(server_id, status="running")
            tokens, errors = _fetch_access_tokens_for_accounts(server, account_ids)
            for error in errors:
                self._append_error(server_id, _clean(error.get("name")), _clean(error.get("error")) or "unknown error")
            current = self._config.get_import_job(server_id) or {}
            self._update_job(
                server_id,
                completed=len(account_ids),
                failed=_nonnegative_int(current.get("failed")),
            )

            if not tokens:
                self._update_job(
                    server_id,
                    status="failed",
                    completed=len(account_ids),
                    failed=_nonnegative_int(current.get("failed")),
                )
                return

            add_result = account_service.add_accounts(tokens, source_type="codex")
            refresh_result = account_service.refresh_accounts(tokens, quiet=True, log_summary=True)
            current = self._config.get_import_job(server_id) or {}
            self._update_job(
                server_id,
                status="completed",
                completed=len(account_ids),
                added=_nonnegative_int(add_result.get("added")),
                skipped=_nonnegative_int(add_result.get("skipped")),
                refreshed=_nonnegative_int(refresh_result.get("refreshed")),
                failed=_nonnegative_int(current.get("failed")),
            )
        except Exception as exc:
            self._append_error(server_id, "job", str(exc) or "unknown error")
            current = self._config.get_import_job(server_id) or {}
            self._update_job(
                server_id,
                status="failed",
                completed=_nonnegative_int(current.get("total")),
                failed=max(1, _nonnegative_int(current.get("failed"))),
            )
        finally:
            if operation_lease is not None:
                operation_lease.release()


sub2api_config = Sub2APIConfig(SUB2API_CONFIG_FILE)
sub2api_import_service = Sub2APIImportService(sub2api_config)
