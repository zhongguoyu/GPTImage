from __future__ import annotations

import asyncio
import json
import itertools
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, StreamingResponse

from services.config import DATA_DIR
from services.log_store import SQLiteLogStore
from services.protocol.error_response import anthropic_error_response, openai_error_response
from utils.helper import anthropic_sse_stream, sse_json_stream
from utils.log import logger

LOG_TYPE_CALL = "call"
LOG_TYPE_ACCOUNT = "account"
INTERNAL_RESPONSE_KEYS = {"_account_email", "_conversation_id"}


class LogService:
    def __init__(self, db_path: Path, legacy_path: Path | None = None):
        self.db_path = Path(db_path)
        self.legacy_path = Path(legacy_path or self.db_path.with_name("logs.jsonl"))
        self.path = self.legacy_path
        self.store = SQLiteLogStore(self.db_path, self.legacy_path)
        self._initialize_lock = threading.RLock()
        self._initialized = False

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._initialize_lock:
            if self._initialized:
                return
            self.store.initialize()
            self._initialized = True

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            self.initialize()

    @staticmethod
    def _sanitize_detail(detail: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(detail)
        for key in (
            "request_text",
            "prompt",
            "response",
            "response_body",
            "response_data",
            "data",
            "b64_json",
            "base64",
            "image_bytes",
        ):
            sanitized.pop(key, None)
        return sanitized

    def add(
        self,
        type: str,
        summary: str = "",
        detail: dict[str, Any] | None = None,
        **data: Any,
    ) -> dict[str, Any] | None:
        payload = detail if detail is not None else data
        if not isinstance(payload, dict):
            payload = {}
        payload = self._sanitize_detail(payload)
        try:
            self._ensure_initialized()
            return self.store.add(type, summary, payload)
        except Exception as exc:
            logger.error({"event": "log_write_failed", "error": str(exc)})
            return None

    def query(
        self,
        type: str = "",
        start_date: str = "",
        end_date: str = "",
        page: int = 1,
        page_size: int = 20,
        *,
        type_filter: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_initialized()
        return self.store.query(
            type_filter=type if type_filter is None else type_filter,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
        )

    def list(self, type: str = "", start_date: str = "", end_date: str = "", limit: int = 200) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        page_size = min(limit, 100)
        return self.query(
            type=type,
            start_date=start_date,
            end_date=end_date,
            page=1,
            page_size=page_size,
        )["items"]

    def delete(self, ids: list[str]) -> int:
        self._ensure_initialized()
        return self.store.delete(ids)

    def cleanup_expired(self, retention_days: int) -> int:
        self._ensure_initialized()
        return self.store.cleanup_expired(retention_days)

    def close(self) -> None:
        with self._initialize_lock:
            self.store.close()
            self._initialized = False

    def start_retention_scheduler(
        self,
        stop_event: threading.Event,
        retention_days_getter,
        *,
        interval_seconds: float = 86400,
    ) -> threading.Thread:
        def run() -> None:
            while not stop_event.wait(interval_seconds):
                try:
                    self.cleanup_expired(int(retention_days_getter()))
                except Exception as exc:
                    logger.error({"event": "log_retention_failed", "error": str(exc)})

        thread = threading.Thread(target=run, name="log-retention", daemon=True)
        thread.start()
        return thread


log_service = LogService(DATA_DIR / "logs.db", DATA_DIR / "logs.jsonl")


def is_safe_to_log_url(url: str) -> bool:
    normalized = url.strip().lower()
    return bool(normalized) and not normalized.startswith("data:")


def _collect_urls(value: object) -> list[str]:
    urls: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "url" and isinstance(item, str) and is_safe_to_log_url(item):
                urls.append(item)
            elif key == "urls" and isinstance(item, list):
                urls.extend(url for url in item if isinstance(url, str) and is_safe_to_log_url(url))
            else:
                urls.extend(_collect_urls(item))
    elif isinstance(value, list):
        for item in value:
            urls.extend(_collect_urls(item))
    return urls


def _collect_account_emails(value: object) -> list[str]:
    emails: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"_account_email", "account_email"} and isinstance(item, str) and item.strip():
                emails.append(item.strip())
            else:
                emails.extend(_collect_account_emails(item))
    elif isinstance(value, list):
        for item in value:
            emails.extend(_collect_account_emails(item))
    return emails


def _collect_conversation_ids(value: object) -> list[str]:
    ids: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "_conversation_id" and isinstance(item, str) and item.strip():
                ids.append(item.strip())
            else:
                ids.extend(_collect_conversation_ids(item))
    elif isinstance(value, list):
        for item in value:
            ids.extend(_collect_conversation_ids(item))
    return ids


def _strip_internal_response_fields(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _strip_internal_response_fields(item)
            for key, item in value.items()
            if key not in INTERNAL_RESPONSE_KEYS
        }
    if isinstance(value, list):
        return [_strip_internal_response_fields(item) for item in value]
    return value


def _image_error_response(exc: Exception) -> JSONResponse:
    from services.protocol.conversation import public_image_error_message

    message = public_image_error_message(str(exc))
    if "no available image quota" in message.lower():
        return openai_error_response(
            {
                "error": {
                    "message": "no available image quota",
                    "type": "insufficient_quota",
                    "param": None,
                    "code": "insufficient_quota",
                }
            },
            429,
        )
    if hasattr(exc, "to_openai_error") and hasattr(exc, "status_code"):
        response = JSONResponse(status_code=int(exc.status_code), content=exc.to_openai_error())
        retry_after = getattr(exc, "retry_after_secs", None)
        if retry_after is not None:
            response.headers["Retry-After"] = str(max(1, int(retry_after)))
        return response
    return openai_error_response(message, 502)


def _protocol_error_response(exc: Exception, status_code: int, sse: str) -> JSONResponse:
    message = str(exc)
    if sse == "anthropic":
        return anthropic_error_response(message, status_code)
    return openai_error_response(message, status_code)


def _scheduler_error_response(exc: Exception, sse: str) -> JSONResponse:
    status_code = int(getattr(exc, "status_code", 503))
    response = _protocol_error_response(exc, status_code, sse)
    retry_after = getattr(exc, "retry_after_seconds", None)
    if retry_after is not None:
        response.headers["Retry-After"] = str(max(1, int(retry_after)))
    return response


async def _close_async_iterator(iterator: object) -> None:
    close = getattr(iterator, "aclose", None)
    if close is None:
        return
    try:
        await close()
    except BaseException:
        pass


def _next_item(items):
    try:
        return True, next(items)
    except StopIteration:
        return False, None
    except BaseException:
        close = getattr(items, "close", None)
        if close is not None:
            try:
                close()
            except BaseException:
                pass
        raise


def _notify_failure(callback: Callable[[], None] | None) -> None:
    if callback is None:
        return
    try:
        callback()
    except BaseException:
        pass


@dataclass
class LoggedCall:
    identity: dict[str, object]
    endpoint: str
    model: str
    summary: str
    started: float = field(default_factory=time.time)
    request_text: str = ""
    request_shape: dict[str, int] | None = None

    async def run(
        self,
        handler,
        *args,
        sse: str = "openai",
        scheduler_runner=None,
        streaming: bool = False,
        on_success: Callable[[object | None], None] | None = None,
        on_failure: Callable[[], None] | None = None,
        on_stream_item: Callable[[object], None] | None = None,
    ):
        from services.protocol.conversation import ImageGenerationError
        from services.scheduler.errors import SchedulerError

        try:
            if scheduler_runner is None:
                result = await run_in_threadpool(handler, *args)
            else:
                result = await scheduler_runner(handler, *args, streaming=streaming)
        except ImageGenerationError as exc:
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc), account_email=getattr(exc, "account_email", ""),
                     conversation_id=getattr(exc, "conversation_id", ""))
            return _image_error_response(exc)
        except HTTPException as exc:
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc.detail))
            raise
        except SchedulerError as exc:
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc))
            headers = {}
            if exc.retry_after_seconds is not None:
                headers["Retry-After"] = str(max(1, int(exc.retry_after_seconds)))
            raise HTTPException(
                status_code=exc.status_code,
                detail={"error": str(exc)},
                headers=headers or None,
            ) from exc
        except Exception as exc:
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc), account_email=getattr(exc, "account_email", ""))
            if self.endpoint.startswith("/v1/images"):
                return _image_error_response(exc)
            return _protocol_error_response(exc, 502, sse)

        if isinstance(result, dict):
            if on_success is not None:
                on_success(result)
            self.log("调用完成", result)
            return _strip_internal_response_fields(result)

        if hasattr(result, "__aiter__"):
            return await self._run_async_stream(
                result,
                sse=sse,
                on_success=on_success,
                on_failure=on_failure,
                on_stream_item=on_stream_item,
            )

        sender = anthropic_sse_stream if sse == "anthropic" else sse_json_stream
        try:
            has_first, first = await run_in_threadpool(_next_item, result)
        except ImageGenerationError as exc:
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc), account_email=getattr(exc, "account_email", ""),
                     conversation_id=getattr(exc, "conversation_id", ""))
            return _image_error_response(exc)
        except HTTPException as exc:
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc.detail))
            raise
        except SchedulerError as exc:
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc))
            raise HTTPException(
                status_code=exc.status_code,
                detail={"error": str(exc)},
                headers=(
                    {"Retry-After": str(max(1, int(exc.retry_after_seconds)))}
                    if exc.retry_after_seconds is not None
                    else None
                ),
            ) from exc
        except Exception as exc:
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc), account_email=getattr(exc, "account_email", ""))
            if self.endpoint.startswith("/v1/images"):
                return _image_error_response(exc)
            return _protocol_error_response(exc, 502, sse)
        if not has_first:
            if on_success is not None:
                on_success(None)
            self.log("流式调用结束")
            return StreamingResponse(sender(()), media_type="text/event-stream")
        return StreamingResponse(
            sender(
                self.stream(
                    itertools.chain([first], result),
                    on_success=on_success,
                    on_failure=on_failure,
                    on_stream_item=on_stream_item,
                )
            ),
            media_type="text/event-stream",
        )

    async def _run_async_stream(
        self,
        result,
        *,
        sse: str,
        on_success: Callable[[object | None], None] | None = None,
        on_failure: Callable[[], None] | None = None,
        on_stream_item: Callable[[object], None] | None = None,
    ):
        from services.protocol.conversation import ImageGenerationError
        from services.scheduler.errors import SchedulerError

        try:
            first = await result.__anext__()
        except StopAsyncIteration:
            await _close_async_iterator(result)
            if on_success is not None:
                on_success(None)
            self.log("流式调用结束")
            return StreamingResponse(
                self._async_sender((), sse=sse),
                media_type="text/event-stream",
            )
        except asyncio.CancelledError:
            await _close_async_iterator(result)
            _notify_failure(on_failure)
            raise
        except ImageGenerationError as exc:
            await _close_async_iterator(result)
            _notify_failure(on_failure)
            self.log(
                "调用失败",
                status="failed",
                error=str(exc),
                account_email=getattr(exc, "account_email", ""),
                conversation_id=getattr(exc, "conversation_id", ""),
            )
            return _image_error_response(exc)
        except SchedulerError as exc:
            await _close_async_iterator(result)
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc))
            return _scheduler_error_response(exc, sse)
        except HTTPException as exc:
            await _close_async_iterator(result)
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc.detail))
            raise
        except Exception as exc:
            await _close_async_iterator(result)
            _notify_failure(on_failure)
            self.log("调用失败", status="failed", error=str(exc))
            if self.endpoint.startswith("/v1/images"):
                return _image_error_response(exc)
            return _protocol_error_response(exc, 502, sse)
        return StreamingResponse(
            self._async_sender(
                self.async_stream(
                    result,
                    first,
                    on_success=on_success,
                    on_failure=on_failure,
                    on_stream_item=on_stream_item,
                ),
                sse=sse,
            ),
            media_type="text/event-stream",
        )

    async def async_stream(
        self,
        items,
        first,
        *,
        on_success: Callable[[object | None], None] | None = None,
        on_failure: Callable[[], None] | None = None,
        on_stream_item: Callable[[object], None] | None = None,
    ):
        urls: list[str] = []
        account_emails: list[str] = []
        conversation_ids: list[str] = []
        failed = False
        try:
            for item in [first]:
                if on_stream_item is not None:
                    on_stream_item(item)
                urls.extend(_collect_urls(item))
                account_emails.extend(_collect_account_emails(item))
                conversation_ids.extend(_collect_conversation_ids(item))
                yield _strip_internal_response_fields(item)
            async for item in items:
                if on_stream_item is not None:
                    on_stream_item(item)
                urls.extend(_collect_urls(item))
                account_emails.extend(_collect_account_emails(item))
                conversation_ids.extend(_collect_conversation_ids(item))
                yield _strip_internal_response_fields(item)
        except BaseException as exc:
            failed = True
            _notify_failure(on_failure)
            if not isinstance(exc, Exception):
                raise
            self.log(
                "流式调用失败",
                status="failed",
                error=str(exc),
                urls=urls,
                account_email=(account_emails[0] if account_emails else getattr(exc, "account_email", "")),
                conversation_id=(conversation_ids[0] if conversation_ids else getattr(exc, "conversation_id", "")),
            )
            raise
        finally:
            close = getattr(items, "aclose", None)
            if close is not None:
                await close()
        if not failed:
            try:
                if on_success is not None:
                    on_success(None)
            except BaseException:
                _notify_failure(on_failure)
                raise
            self.log(
                "流式调用结束",
                urls=urls,
                account_email=account_emails[0] if account_emails else "",
                conversation_id=conversation_ids[0] if conversation_ids else "",
            )

    @staticmethod
    async def _async_sender(items, *, sse: str):
        if sse == "openai":
            yield ": stream-open\n\n"
        try:
            try:
                if hasattr(items, "__aiter__"):
                    iterator = items.__aiter__()
                    async for item in iterator:
                        if sse == "anthropic":
                            event = str(item.get("type") or "message_delta") if isinstance(item, dict) else "message_delta"
                            yield f"event: {event}\n"
                            yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
                        else:
                            yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
                else:
                    for item in items:
                        if sse == "anthropic":
                            event = str(item.get("type") or "message_delta") if isinstance(item, dict) else "message_delta"
                            yield f"event: {event}\n"
                            yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
                        else:
                            yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
            except Exception as exc:
                if sse == "anthropic":
                    error = {"type": "error", "error": {"type": exc.__class__.__name__, "message": str(exc)}}
                    yield "event: error\n"
                    yield f"data: {json.dumps(error, ensure_ascii=False)}\n\n"
                else:
                    error = exc.to_openai_error() if hasattr(exc, "to_openai_error") else {
                        "error": {"message": str(exc), "type": exc.__class__.__name__}
                    }
                    yield f"data: {json.dumps(error, ensure_ascii=False)}\n\n"
            if sse == "openai":
                yield "data: [DONE]\n\n"
        finally:
            await _close_async_iterator(items)

    def stream(
        self,
        items,
        *,
        on_success: Callable[[object | None], None] | None = None,
        on_failure: Callable[[], None] | None = None,
        on_stream_item: Callable[[object], None] | None = None,
    ):
        urls: list[str] = []
        account_emails: list[str] = []
        conversation_ids: list[str] = []
        failed = False
        try:
            for item in items:
                if on_stream_item is not None:
                    on_stream_item(item)
                urls.extend(_collect_urls(item))
                account_emails.extend(_collect_account_emails(item))
                conversation_ids.extend(_collect_conversation_ids(item))
                yield _strip_internal_response_fields(item)
        except BaseException as exc:
            failed = True
            _notify_failure(on_failure)
            if not isinstance(exc, Exception):
                raise
            self.log(
                "流式调用失败",
                status="failed",
                error=str(exc),
                urls=urls,
                account_email=(account_emails[0] if account_emails else getattr(exc, "account_email", "")),
                conversation_id=(conversation_ids[0] if conversation_ids else getattr(exc, "conversation_id", "")),
            )
            if self.endpoint.startswith("/v1/images") and not hasattr(exc, "to_openai_error"):
                from services.protocol.conversation import ImageGenerationError, public_image_error_message

                raise ImageGenerationError(public_image_error_message(str(exc))) from exc
            raise
        if not failed:
            try:
                if on_success is not None:
                    on_success(None)
            except BaseException:
                _notify_failure(on_failure)
                raise
            self.log("流式调用结束", urls=urls, account_email=account_emails[0] if account_emails else "",
                     conversation_id=conversation_ids[0] if conversation_ids else "")

    def log(self, suffix: str, result: object = None, status: str = "success", error: str = "",
            urls: list[str] | None = None, account_email: str = "", conversation_id: str = "") -> None:
        detail = {
            "key_id": self.identity.get("id"),
            "key_name": self.identity.get("name"),
            "role": self.identity.get("role"),
            "endpoint": self.endpoint,
            "model": self.model,
            "started_at": datetime.fromtimestamp(self.started).strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_ms": int((time.time() - self.started) * 1000),
            "status": status,
        }
        if self.request_shape:
            detail["request_shape"] = self.request_shape
        if error:
            detail["error"] = error
        email = str(account_email or "").strip()
        if not email:
            emails = _collect_account_emails(result)
            email = emails[0] if emails else ""
        if email:
            detail["account_email"] = email
        conv_id = str(conversation_id or "").strip()
        if not conv_id:
            conv_ids = _collect_conversation_ids(result)
            conv_id = conv_ids[0] if conv_ids else ""
        if conv_id:
            detail["conversation_id"] = conv_id
        collected_urls = [
            url
            for url in [*(urls or []), *_collect_urls(result)]
            if is_safe_to_log_url(url)
        ]
        if collected_urls and not self.endpoint.startswith("/v1/search"):
            detail["urls"] = list(dict.fromkeys(collected_urls))
        log_service.add(LOG_TYPE_CALL, f"{self.summary}{suffix}", detail)
