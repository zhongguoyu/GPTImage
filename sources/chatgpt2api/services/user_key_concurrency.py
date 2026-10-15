from __future__ import annotations

from collections.abc import Callable, Mapping
from threading import Lock
from typing import Literal

from services.config import config


RequestType = Literal["text", "image"]
_REQUEST_TYPES = frozenset(("text", "image"))


class UserKeyConcurrencyLimitError(Exception):
    status_code = 429
    retry_after_seconds = 1

    def __init__(self, request_type: RequestType):
        if request_type not in _REQUEST_TYPES:
            raise ValueError(f"unsupported request type: {request_type}")
        self.request_type = request_type
        super().__init__(f"user key {request_type} concurrency limit exceeded")


class UserKeyConcurrencyLease:
    def __init__(
        self,
        limiter: UserKeyConcurrencyLimiter | None,
        key: tuple[str, RequestType] | None,
    ):
        self._limiter = limiter
        self._key = key
        self._release_lock = Lock()
        self._released = False

    def release(self) -> None:
        with self._release_lock:
            if self._released:
                return
            self._released = True
        if self._limiter is not None and self._key is not None:
            self._limiter._release(self._key)


class UserKeyConcurrencyLimiter:
    def __init__(
        self,
        *,
        text_limit_getter: Callable[[], object] | None = None,
        image_limit_getter: Callable[[], object] | None = None,
        limit_getter: Callable[[Mapping[str, object], RequestType], object] | None = None,
    ):
        self._identity_limit_getter = limit_getter
        self._limit_getters: dict[RequestType, Callable[[], object]] = {
            "text": text_limit_getter or (lambda: config.user_key_text_concurrency),
            "image": image_limit_getter or (lambda: config.user_key_image_concurrency),
        }
        self._active: dict[tuple[str, RequestType], int] = {}
        self._lock = Lock()

    @staticmethod
    def _validate_request_type(request_type: RequestType) -> RequestType:
        if request_type not in _REQUEST_TYPES:
            raise ValueError(f"unsupported request type: {request_type}")
        return request_type

    @staticmethod
    def _key(identity: Mapping[str, object], request_type: RequestType) -> tuple[str, RequestType] | None:
        if identity.get("role") != "user":
            return None
        user_id = str(identity.get("id") or "").strip()
        if not user_id:
            return None
        return user_id, request_type

    def _limit(self, identity: Mapping[str, object], request_type: RequestType) -> int:
        try:
            value = (
                self._identity_limit_getter(identity, request_type)
                if self._identity_limit_getter is not None
                else self._limit_getters[request_type]()
            )
            return max(1, int(value))
        except (OverflowError, TypeError, ValueError):
            return 1

    def try_acquire(
        self,
        identity: Mapping[str, object],
        request_type: RequestType,
    ) -> UserKeyConcurrencyLease | None:
        request_type = self._validate_request_type(request_type)
        key = self._key(identity, request_type)
        if key is None:
            return UserKeyConcurrencyLease(None, None)
        limit = self._limit(identity, request_type)
        with self._lock:
            active = self._active.get(key, 0)
            if active >= limit:
                return None
            self._active[key] = active + 1
        return UserKeyConcurrencyLease(self, key)

    def acquire_existing(
        self,
        identity: Mapping[str, object],
        request_type: RequestType,
    ) -> UserKeyConcurrencyLease:
        request_type = self._validate_request_type(request_type)
        key = self._key(identity, request_type)
        if key is None:
            return UserKeyConcurrencyLease(None, None)
        with self._lock:
            self._active[key] = self._active.get(key, 0) + 1
        return UserKeyConcurrencyLease(self, key)

    def _release(self, key: tuple[str, RequestType]) -> None:
        with self._lock:
            active = self._active.get(key, 0)
            if active <= 1:
                self._active.pop(key, None)
            else:
                self._active[key] = active - 1

    def active_count(self, identity: Mapping[str, object], request_type: RequestType) -> int:
        request_type = self._validate_request_type(request_type)
        key = self._key(identity, request_type)
        if key is None:
            return 0
        with self._lock:
            return self._active.get(key, 0)

def _database_limit_getter(identity: Mapping[str, object], request_type: RequestType) -> object:
    try:
        from services.quota_service import quota_service

        return quota_service.get_concurrency_limit(str(identity.get("id") or ""), request_type)
    except Exception:
        return (
            config.user_key_text_concurrency
            if request_type == "text"
            else config.user_key_image_concurrency
        )


user_key_concurrency = UserKeyConcurrencyLimiter(limit_getter=_database_limit_getter)
