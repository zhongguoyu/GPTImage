from __future__ import annotations

import base64
import inspect
import json
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator

import tiktoken

from services.account_service import account_service
from services.config import config
from services.image_comments import ImageComment
from services.openai_backend_api import (
    IMAGE_RATE_LIMIT_PAYLOAD_RE,
    ImageContentPolicyError,
    ImagePollTimeoutError,
    ImageRateLimitedError,
    OpenAIBackendAPI,
)
from services.scheduler.context import current_execution_context
from services.scheduler.models import Lane, Outcome
from utils.helper import (
    IMAGE_MODELS,
    anonymize_token,
    extract_image_from_message_content,
    is_codex_image_model,
    is_supported_image_model,
    MessageImageBudget,
    split_image_model,
)
from utils.image_tokens import count_image_content_tokens
from utils.log import logger


class ImageGenerationError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int = 502,
        error_type: str = "server_error",
        code: str | None = "upstream_error",
        param: str | None = None,
        account_email: str = "",
        conversation_id: str = "",
        retry_after_secs: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type
        self.code = code
        self.param = param
        self.account_email = account_email
        self.conversation_id = conversation_id
        self.retry_after_secs = retry_after_secs

    def to_openai_error(self) -> dict[str, Any]:
        error_dict = {
            "error": {
                "message": public_image_error_message(str(self)),
                "type": self.error_type,
                "param": self.param,
                "code": self.code,
            }
        }
        if self.account_email:
            error_dict["error"]["account_email"] = self.account_email
        if self.retry_after_secs is not None:
            error_dict["error"]["retry_after"] = int(self.retry_after_secs)
        return error_dict


def public_image_error_message(message: str) -> str:
    text = str(message or "").strip()
    lower = text.lower()
    if any(item in lower for item in ("backend-api/", "status=", "body=", "chatgpt.com", "upstreamhttperror")):
        return "The image generation request failed. Please try again later."
    return text or "The image generation request failed. Please try again later."


def is_token_invalid_error(message: str) -> bool:
    text = str(message or "").lower()
    return (
        "token_invalidated" in text
        or "token_revoked" in text
        or "authentication token has been invalidated" in text
        or "invalidated oauth token" in text
    )


def is_tls_connection_error(message: str) -> bool:
    """检测 TLS/SSL 连接错误，这类错误通常可以通过重试解决。"""
    text = str(message or "").lower()
    return (
        "curl: (35)" in text
        or "tls connect error" in text
        or "openssl_internal" in text
        or "ssl: wrong_version_number" in text
        or "ssl: certificate_verify_failed" in text
        or "connection aborted" in text
        or "remote disconnected" in text
        or "connection reset by peer" in text
    )


def is_connection_timeout_error(message: str) -> bool:
    """检测连接超时错误（如 curl 28），这类错误可通过同账号短等待重试解决。"""
    text = str(message or "").lower()
    return (
        "curl: (28)" in text
        or "operation timed out" in text
        or "connection timed out" in text
        or "read timed out" in text
        or "connect timeout" in text
    )


def image_stream_error_message(message: str) -> str:
    text = str(message or "")
    if is_token_invalid_error(text):
        return "image generation failed"
    if is_tls_connection_error(text):
        return "upstream image connection failed, please retry later"
    if is_connection_timeout_error(text):
        return "upstream connection timed out, please retry later"
    return text or "image generation failed"


REFERENCED_IMAGE_IDS_RE = re.compile(r'"referenced_image_ids"\s*:\s*\[([^\]]+)\]')
# 检测模型返回的部分工具调用 JSON（如 {"size":"1920x1088","n":1}）
# 这些 JSON 包含图片生成工具的参数，但没有实际生成图片
TOOL_PARAMS_JSON_RE = re.compile(
    r'\{\s*"size"\s*:\s*"\d+x\d+"\s*,\s*"n"\s*:\s*\d+\s*\}'
)


def is_model_text_reply_instead_of_image(message: str) -> bool:
    """检测模型是否返回了文本回复（包含工具调用 JSON）而非实际生成图片。

    当上游 ChatGPT 未能触发图片生成工具时，会返回一段描述性文本，
    其中可能包含 JSON 参数（如 prompt、referenced_image_ids、size/n 等）。
    这种情况应被视为「上游未生成图片」而非「内容策略违规」。

    检测两种模式：
    1. 完整的工具调用 JSON（含 referenced_image_ids）
    2. 部分的工具参数 JSON（如 {"size":"1920x1088","n":1}）
    """
    if not message:
        return False
    if REFERENCED_IMAGE_IDS_RE.search(message):
        return True
    # 检测部分工具参数 JSON（模型返回了工具参数但未触发工具）
    if TOOL_PARAMS_JSON_RE.search(message):
        return True
    return False


def encode_images(images: Iterable[tuple[bytes, str, str]]) -> list[str]:
    return [base64.b64encode(data).decode("ascii") for data, _, _ in images if data]


def message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and str(item.get("type") or "") in {"text", "input_text", "output_text"}:
                parts.append(str(item.get("text") or ""))
        return "".join(parts)
    return ""


def normalize_messages(messages: object, system: Any = None) -> list[dict[str, Any]]:
    normalized = []
    image_budget = MessageImageBudget(max(1, int(config.image_input_max_bytes)))
    if config.global_system_prompt:
        normalized.append({"role": "system", "content": config.global_system_prompt})
    system_text = message_text(system)
    if system_text:
        normalized.append({"role": "system", "content": system_text})
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            role = message.get("role", "user")
            content = message.get("content", "")
            text = message_text(content)
            images: list[tuple[bytes, str]] = []
            if role == "user":
                images.extend(extract_image_from_message_content(content, budget=image_budget))
                if isinstance(content, list):
                    for part in content:
                        if not isinstance(part, dict) or part.get("type") != "image":
                            continue
                        data = part.get("data")
                        if isinstance(data, (bytes, bytearray)) and all(existing[0] != bytes(data) for existing in images):
                            images.append((image_budget.consume(bytes(data)), str(part.get("mime") or "image/png")))
            if images:
                parts: list[Any] = []
                if text:
                    parts.append({"type": "text", "text": text})
                for data, mime in images:
                    parts.append({"type": "image", "data": data, "mime": mime})
                normalized.append({"role": role, "content": parts})
            else:
                normalized.append({"role": role, "content": text})
    return normalized


def prompt_with_global_system(prompt: str) -> str:
    return f"{config.global_system_prompt}\n\n{prompt}" if config.global_system_prompt else prompt


def assistant_history_text(messages: list[dict[str, Any]]) -> str:
    return "".join(str(item.get("content") or "") for item in messages if item.get("role") == "assistant")


def assistant_history_messages(messages: list[dict[str, Any]]) -> list[str]:
    return [str(item.get("content") or "") for item in messages if item.get("role") == "assistant" and item.get("content")]


def build_image_prompt(prompt: str, size: str | None, quality: str = "auto") -> str:
    hints = []
    if size:
        hints.append(f"输出图片尺寸为 {size}。")
    if quality:
        hints.append(f"输出图片质量为 {quality}。")
    return f"{prompt.strip()}\n\n{''.join(hints)}" if hints else prompt


def encoding_for_model(model: str):
    try:
        return tiktoken.encoding_for_model(model)
    except KeyError:
        try:
            return tiktoken.get_encoding("o200k_base")
        except KeyError:
            return tiktoken.get_encoding("cl100k_base")


def count_message_image_tokens(messages: list[dict[str, Any]], model: str) -> int:
    return sum(count_image_content_tokens(message.get("content"), model) for message in messages)


def count_message_text_tokens(messages: list[dict[str, Any]], model: str) -> int:
    encoding = encoding_for_model(model)
    total = 0
    for message in messages:
        total += 3
        for key, value in message.items():
            if key == "content" and isinstance(value, list):
                total += len(encoding.encode(message_text(value)))
            elif isinstance(value, str):
                total += len(encoding.encode(value))
            else:
                continue
            if key == "name":
                total += 1
    return total + 3


def count_message_tokens(messages: list[dict[str, Any]], model: str) -> int:
    return count_message_text_tokens(messages, model) + count_message_image_tokens(messages, model)


def count_text_tokens(text: str, model: str) -> int:
    return len(encoding_for_model(model).encode(text))


def format_image_result(
    items: list[dict[str, Any]],
    prompt: str,
    response_format: str,
    created: int | None = None,
    message: str = "",
) -> dict[str, Any]:
    from services.routing_mode import native_selected
    if native_selected() and any(item.get("b64_json") and not item.get("url") for item in items):
        from services.protocol.image_upstream import _archive_items
        items = [(_archive_items([item], {})[0] if item.get("b64_json") and not item.get("url") else item) for item in items]
    data: list[dict[str, Any]] = []
    for item in items:
        revised_prompt = str(item.get("revised_prompt") or prompt).strip() or prompt
        url = str(item.get("url") or "").strip()
        if response_format != "b64_json" and url:
            data.append({
                "url": url,
                "revised_prompt": revised_prompt,
            })
            continue
        b64_json = str(item.get("b64_json") or "").strip()
        if not b64_json:
            continue
        if response_format == "b64_json":
            data.append({
                "b64_json": b64_json,
                "revised_prompt": revised_prompt,
            })
        else:
            data.append({
                "url": f"data:image/png;base64,{b64_json}",
                "revised_prompt": revised_prompt,
            })
    result: dict[str, Any] = {"created": created or int(time.time()), "data": data}
    if message and not data:
        result["message"] = message
    return result


def image_result_items_from_urls(
    backend: OpenAIBackendAPI,
    image_urls: list[str],
    response_format: str,
) -> list[dict[str, str]]:
    # ChatGPT estuary links require the generating account's authentication.
    # Materialize them while that backend session is still available so the
    # existing response/local-storage pipeline can serve browser-visible images.
    from urllib.parse import urlparse
    authenticated_links = any(
        urlparse(url).hostname == "chatgpt.com"
        and urlparse(url).path.startswith("/backend-api/estuary/content")
        for url in image_urls
    )
    from services.routing_mode import native_selected
    if response_format != "b64_json" and not authenticated_links and not native_selected():
        return [{"url": url} for url in image_urls if str(url or "").strip()]
    return [
        {"b64_json": base64.b64encode(image_data).decode("ascii")}
        for image_data in backend.download_image_bytes(image_urls)
    ]


@dataclass
class ConversationRequest:
    model: str = "auto"
    prompt: str = ""
    messages: list[dict[str, Any]] | None = None
    thinking_effort: str = ""
    images: list[str] | None = None
    n: int = 1
    size: str | None = None
    quality: str = "auto"
    response_format: str = "b64_json"
    message_as_error: bool = False
    progress_callback: Any = None  # Callable[[str, str], None] | None
    comments: list[ImageComment] | None = None  # 图片锚点评论（编辑场景）
    markup: bool = False  # 标注画笔编辑（输入图已含笔迹像素）


def _report_image_progress(
    request: ConversationRequest,
    step: str,
    conversation_id: str = "",
) -> None:
    callback = request.progress_callback
    if callback is None:
        return
    if conversation_id:
        try:
            signature = inspect.signature(callback)
        except (TypeError, ValueError):
            callback(step, conversation_id)
            return
        try:
            signature.bind(step, conversation_id)
        except TypeError:
            # Keep compatibility with integrations that still accept one arg.
            callback(step)
        else:
            callback(step, conversation_id)
        return
    callback(step)


@dataclass
class ConversationState:
    text: str = ""
    raw_text: str = ""
    conversation_id: str = ""
    file_ids: list[str] = field(default_factory=list)
    sediment_ids: list[str] = field(default_factory=list)
    blocked: bool = False
    tool_invoked: bool | None = None
    turn_use_case: str = ""
    rate_limited: bool = False


@dataclass
class ImageOutput:
    kind: str
    model: str
    index: int
    total: int
    created: int = field(default_factory=lambda: int(time.time()))
    text: str = ""
    upstream_event_type: str = ""
    data: list[dict[str, Any]] = field(default_factory=list)
    account_email: str = ""
    conversation_id: str = ""

    def to_chunk(self) -> dict[str, Any]:
        chunk: dict[str, Any] = {
            "object": "image.generation.chunk",
            "created": self.created,
            "model": self.model,
            "index": self.index,
            "total": self.total,
            "progress_text": self.text,
            "upstream_event_type": self.upstream_event_type,
            "data": [],
        }
        if self.account_email:
            chunk["_account_email"] = self.account_email
        if self.conversation_id:
            chunk["_conversation_id"] = self.conversation_id
        if self.kind == "message":
            chunk.update({
                "object": "image.generation.message",
                "message": self.text,
            })
            chunk.pop("progress_text", None)
            chunk.pop("upstream_event_type", None)
        elif self.kind == "result":
            chunk.update({
                "object": "image.generation.result",
                "data": self.data,
            })
            chunk.pop("progress_text", None)
            chunk.pop("upstream_event_type", None)
        return chunk


def assistant_message_text(message: dict[str, Any]) -> str:
    content = message.get("content") or {}
    parts = content.get("parts") or []
    if isinstance(parts, list) and parts:
        text = "".join(part for part in parts if isinstance(part, str))
        if text:
            return text
    # Fallback: content_type "code" stores text in the "text" field instead of "parts"
    text_field = str(content.get("text") or "")
    if text_field:
        return text_field
    return ""


def is_visible_assistant_message(message: dict[str, Any]) -> bool:
    """Return whether an upstream assistant message is intended for the user."""
    author = message.get("author")
    if not isinstance(author, dict):
        return False
    role = str(author.get("role") or "").strip().lower()
    if role != "assistant":
        return False

    metadata = message.get("metadata") or {}
    if isinstance(metadata, dict) and metadata.get("is_visually_hidden_from_conversation") is True:
        return False

    # Tool commands are emitted as assistant messages addressed to recipients
    # such as "web". Only messages addressed to everyone are user-visible.
    recipient = str(message.get("recipient") or "").strip().lower()
    if recipient and recipient != "all":
        return False

    # Reasoning and other internal channels must not leak into API text output.
    channel = str(message.get("channel") or "").strip().lower()
    if channel and channel != "final":
        return False

    return True


def strip_history(text: str, history_text: str = "") -> str:
    text = str(text or "")
    history_text = str(history_text or "")
    while history_text and text.startswith(history_text):
        text = text[len(history_text):]
    return text


def sanitize_output_text(text: str) -> str:
    text = str(text or "")

    def is_internal_annotation_part(part: str) -> bool:
        value = part.strip()
        if not value:
            return True
        lower = value.lower()
        return bool(
            re.fullmatch(r"turn\d+[a-z]*\d*", lower)
            or re.fullmatch(r"turn\d+\w*", lower)
            or lower.startswith(("turn", "source", "sources"))
        )

    def readable_annotation_part(parts: list[str]) -> str:
        for part in parts:
            value = part.strip()
            if value and not is_internal_annotation_part(value):
                return value
        return ""

    def annotation_text(payload: str) -> str:
        parts = [part.strip() for part in payload.split("\ue202")]
        kind = (parts[0] if parts else "").lower()
        data = parts[1:]
        if kind == "url":
            label = data[0] if data else ""
            url = data[1] if len(data) > 1 else ""
            if label and url.startswith(("http://", "https://")):
                return f"{label} ({url})"
            return label or url
        if kind == "cite":
            return readable_annotation_part(data)
        return readable_annotation_part(data)

    def replace_annotation(match: re.Match[str]) -> str:
        return annotation_text(match.group(1))

    def replace_annotation_before_punctuation(match: re.Match[str]) -> str:
        leading_space = match.group(1)
        replacement = annotation_text(match.group(2))
        return f"{leading_space}{replacement}" if replacement else ""

    # ChatGPT web sometimes returns rich annotation markers using private-use
    # characters. API clients cannot render those. Preserve readable labels
    # from entity/link annotations, while removing internal citation pointers.
    text = re.sub(r"(\s*)\ue200([^\ue201]*)\ue201(?=[.,;:!?])", replace_annotation_before_punctuation, text)
    text = re.sub(r"\ue200([^\ue201]*)\ue201", replace_annotation, text)
    text = re.sub(r"\ue200[^\ue201]*$", "", text)
    return text


def assistant_raw_text(event: dict[str, Any], current_text: str = "", history_text: str = "") -> str:
    for candidate in (event, event.get("v")):
        if not isinstance(candidate, dict):
            continue
        message = candidate.get("message")
        if not isinstance(message, dict) or not is_visible_assistant_message(message):
            continue
        text = assistant_message_text(message)
        if text:
            return strip_history(text, history_text)
    return apply_text_patch(event, current_text, history_text)


def assistant_text(event: dict[str, Any], current_text: str = "", history_text: str = "") -> str:
    return sanitize_output_text(assistant_raw_text(event, current_text, history_text))


def event_assistant_text(event: dict[str, Any], history_text: str = "") -> str:
    for candidate in (event, event.get("v")):
        if not isinstance(candidate, dict):
            continue
        message = candidate.get("message")
        if isinstance(message, dict) and is_visible_assistant_message(message):
            return strip_history(assistant_message_text(message), history_text)
    return ""


def apply_text_patch(event: dict[str, Any], current_text: str = "", history_text: str = "") -> str:
    if event.get("p") == "/message/content/parts/0":
        return apply_patch_op(event, current_text, history_text)

    operations = event.get("v")
    if isinstance(operations, str) and current_text and not event.get("p") and not event.get("o"):
        return current_text + operations

    if event.get("o") == "patch" and isinstance(operations, list):
        text = current_text
        for item in operations:
            if isinstance(item, dict):
                text = apply_text_patch(item, text, history_text)
        return text

    if not isinstance(operations, list):
        return current_text

    text = current_text
    for item in operations:
        if isinstance(item, dict):
            text = apply_text_patch(item, text, history_text)
    return text


def apply_patch_op(operation: dict[str, Any], current_text: str, history_text: str = "") -> str:
    op = operation.get("o")
    value = str(operation.get("v") or "")
    if op == "append":
        return current_text + value
    if op == "replace":
        return strip_history(value, history_text)
    return current_text


def add_unique(values: list[str], candidates: list[str]) -> None:
    for candidate in candidates:
        if candidate and candidate not in values:
            values.append(candidate)


FILE_SERVICE_ID_RE = re.compile(r"file-service://([A-Za-z0-9_-]+)")
FILE_ID_RE = re.compile(r"\b(file[-_](?!service\b)[A-Za-z0-9_-]+)\b")
# 真正的图片文件 ID 格式：file_00000000 + 24位十六进制字符（共32字符）
# 用于过滤非图片文件 ID（如 file_upload_business_upsell）
REAL_IMAGE_FILE_ID_RE = re.compile(r"\bfile_00000000[a-f0-9]{24}\b")
SEDIMENT_ID_RE = re.compile(r"sediment://([A-Za-z0-9_-]+)")


def extract_conversation_ids(payload: str) -> tuple[str, list[str], list[str]]:
    conversation_match = re.search(r'"conversation_id"\s*:\s*"([^"]+)"', payload)
    conversation_id = conversation_match.group(1) if conversation_match else ""
    file_ids: list[str] = []
    # Negative lookahead excludes "file-service" (URI prefix, not a real id).
    add_unique(file_ids, FILE_SERVICE_ID_RE.findall(payload))
    # 只提取真正的图片文件 ID（file_00000000... 格式），过滤非图片文件 ID（如 file_upload_business_upsell）
    add_unique(file_ids, REAL_IMAGE_FILE_ID_RE.findall(payload))
    sediment_ids = SEDIMENT_ID_RE.findall(payload)
    return conversation_id, file_ids, sediment_ids


def is_image_tool_event(event: dict[str, Any]) -> bool:
    value = event.get("v")
    message = event.get("message") or (value.get("message") if isinstance(value, dict) else None)
    if not isinstance(message, dict):
        return False
    metadata = message.get("metadata") or {}
    author = message.get("author") or {}
    content = message.get("content") or {}
    if author.get("role") != "tool":
        return False
    if metadata.get("async_task_type") == "image_gen":
        return True
    if content.get("content_type") != "multimodal_text":
        return False
    return any(
        isinstance(part, dict) and (
                part.get("content_type") == "image_asset_pointer"
                or str(part.get("asset_pointer") or "").startswith(("file-service://", "sediment://"))
        )
        for part in content.get("parts") or []
    )


def _is_user_message_event(event: dict[str, Any]) -> bool:
    """检查事件是否来自 user 角色消息。"""
    value = event.get("v")
    message = event.get("message") or (value.get("message") if isinstance(value, dict) else None)
    if isinstance(message, dict):
        author = message.get("author") or {}
        if str(author.get("role") or "").strip().lower() == "user":
            return True
    return False


def update_conversation_state(state: ConversationState, payload: str, event: dict[str, Any] | None = None) -> None:
    # 滑动窗口限流标记：image_gen 工具返回 "You're generating images too quickly..."
    # 可能以 patch/delta 形式分片到达，直接在原始 payload 上匹配最可靠。
    if not state.rate_limited and IMAGE_RATE_LIMIT_PAYLOAD_RE.search(payload or ""):
        state.rate_limited = True
    conversation_id, file_ids, sediment_ids = extract_conversation_ids(payload)
    if conversation_id and not state.conversation_id:
        state.conversation_id = conversation_id
    # Accept file_id / sediment_id when any of:
    #   1) event is a complete image_gen tool message
    #   2) prior server_ste_metadata already flipped tool_invoked True (in an image_gen turn),
    #      BUT only for non-user messages — user messages contain the uploaded input image
    #      which must NOT be treated as a generated output.
    #   3) patch event whose payload references asset_pointer / file-service://,
    #      BUT only when the event is not a user message.
    is_patch_event = isinstance(event, dict) and event.get("o") == "patch"
    is_user_msg = isinstance(event, dict) and _is_user_message_event(event)
    image_context = (
        (isinstance(event, dict) and is_image_tool_event(event))
        or (state.tool_invoked is True and not is_user_msg)
        or (is_patch_event and not is_user_msg and ("asset_pointer" in payload or "file-service://" in payload))
    )
    if image_context:
        add_unique(state.file_ids, file_ids)
        add_unique(state.sediment_ids, sediment_ids)
    if not isinstance(event, dict):
        return
    state.conversation_id = str(event.get("conversation_id") or state.conversation_id)
    value = event.get("v")
    if isinstance(value, dict):
        state.conversation_id = str(value.get("conversation_id") or state.conversation_id)
    if event.get("type") == "moderation":
        moderation = event.get("moderation_response")
        if isinstance(moderation, dict) and moderation.get("blocked") is True:
            state.blocked = True
    if event.get("type") == "server_ste_metadata":
        metadata = event.get("metadata")
        if isinstance(metadata, dict):
            if isinstance(metadata.get("tool_invoked"), bool):
                state.tool_invoked = metadata["tool_invoked"]
            state.turn_use_case = str(metadata.get("turn_use_case") or state.turn_use_case)


def conversation_base_event(event_type: str, state: ConversationState, **extra: Any) -> dict[str, Any]:
    return {
        "type": event_type,
        "text": state.text,
        "conversation_id": state.conversation_id,
        "file_ids": list(state.file_ids),
        "sediment_ids": list(state.sediment_ids),
        "blocked": state.blocked,
        "tool_invoked": state.tool_invoked,
        "turn_use_case": state.turn_use_case,
        "rate_limited": state.rate_limited,
        **extra,
    }


def iter_conversation_payloads(payloads: Iterator[str], history_text: str = "",
                               history_messages: list[str] | None = None) -> Iterator[dict[str, Any]]:
    state = ConversationState()
    history_messages = history_messages or []
    history_index = 0
    for payload in payloads:
        # print(f"[upstream_sse] {payload}", flush=True)
        if not payload:
            continue
        if payload == "[DONE]":
            yield conversation_base_event("conversation.done", state, done=True)
            break
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            update_conversation_state(state, payload)
            yield conversation_base_event("conversation.raw", state, payload=payload)
            continue
        if not isinstance(event, dict):
            yield conversation_base_event("conversation.event", state, raw=event)
            continue
        update_conversation_state(state, payload, event)
        if history_index < len(history_messages) and event_assistant_text(event, history_text) == history_messages[history_index]:
            history_index += 1
            state.raw_text = ""
            state.text = ""
            continue
        next_raw_text = assistant_raw_text(event, state.raw_text, history_text)
        next_text = sanitize_output_text(next_raw_text)
        state.raw_text = next_raw_text
        if next_text != state.text:
            delta = next_text[len(state.text):] if next_text.startswith(state.text) else next_text
            state.text = next_text
            yield conversation_base_event("conversation.delta", state, raw=event, delta=delta)
            continue
        yield conversation_base_event("conversation.event", state, raw=event)


def conversation_events(
    backend: OpenAIBackendAPI,
    messages: list[dict[str, Any]] | None = None,
    model: str = "auto",
    prompt: str = "",
    images: list[str] | None = None,
    size: str | None = None,
    quality: str = "auto",
    thinking_effort: str = "",
    comments: list[ImageComment] | None = None,
    markup: bool = False,
) -> Iterator[dict[str, Any]]:
    normalized = normalize_messages(messages or ([{"role": "user", "content": prompt}] if prompt else []))
    image_model = is_supported_image_model(model)
    history_text = "" if image_model else assistant_history_text(normalized)
    history_messages = [] if image_model else assistant_history_messages(normalized)
    # 纯评论/纯标注编辑（无全局 prompt）时保持与网页端一致：文本部分只含必要内容，不追加尺寸提示
    if image_model and (comments or markup) and not prompt.strip():
        final_prompt = ""
    else:
        final_prompt = prompt_with_global_system(build_image_prompt(prompt, size, quality)) if image_model else prompt
    payloads = backend.stream_conversation(
        messages=normalized,
        model=model,
        prompt=final_prompt,
        images=images if image_model else None,
        system_hints=["picture_v2"] if image_model else None,
        thinking_effort=thinking_effort if not image_model else "",
        comments=comments if image_model else None,
        markup=markup if image_model else False,
    )
    yield from iter_conversation_payloads(payloads, history_text, history_messages)


def text_backend(model: str = "auto") -> OpenAIBackendAPI:
    context = current_execution_context()
    if context is not None and context.lease is not None:
        return OpenAIBackendAPI(access_token=context.lease.access_token)
    return OpenAIBackendAPI(access_token=account_service.get_text_access_token(model=model))


def stream_text_deltas(backend: OpenAIBackendAPI, request: ConversationRequest) -> Iterator[str]:
    attempted_tokens: set[str] = set()
    execution_context = current_execution_context()
    token = getattr(backend, "access_token", "")
    emitted = False
    while True:
        if token and token in attempted_tokens:
            raise RuntimeError("no available text account")
        if token:
            attempted_tokens.add(token)
        active_backend = None
        try:
            active_backend = OpenAIBackendAPI(access_token=token)
            for event in conversation_events(
                active_backend,
                messages=request.messages,
                model=request.model,
                prompt=request.prompt,
                thinking_effort=request.thinking_effort,
            ):
                if event.get("type") != "conversation.delta":
                    continue
                delta = str(event.get("delta") or "")
                if delta:
                    emitted = True
                    yield delta
            if execution_context is None:
                account_service.mark_text_used(token)
            return
        except Exception as exc:
            error_message = str(exc)
            if token and not emitted and is_token_invalid_error(error_message):
                if execution_context is not None and execution_context.replace_lease is not None:
                    replacement = execution_context.replace_lease(
                        execution_context,
                        set(attempted_tokens),
                        Outcome.AUTH_ERROR,
                    )
                    if replacement is None:
                        raise RuntimeError("no available text account") from exc
                    token = replacement.access_token
                    continue
                refreshed_token = account_service.refresh_access_token(token, force=True, event="text_stream")
                if refreshed_token and refreshed_token != token and refreshed_token not in attempted_tokens:
                    token = refreshed_token
                else:
                    account_service.remove_invalid_token(token, "text_stream")
                    token = account_service.get_text_access_token(
                        excluded_tokens=set(attempted_tokens),
                        model=request.model,
                    )
                if token:
                    continue
            raise
        finally:
            if active_backend is not None:
                active_backend.close()


def collect_text(backend: OpenAIBackendAPI, request: ConversationRequest) -> str:
    return "".join(stream_text_deltas(backend, request))


def _get_detailed_error_from_tasks(
    backend: OpenAIBackendAPI,
    conversation_id: str,
    timeout_secs: float = 10.0,
    wait_secs: float = 2.0,
) -> str:
    """从 /backend-api/tasks/ 接口获取结构化错误信息。

    当 SSE 流检测到 moderation 拦截时，轮询 tasks 接口获取详细错误文本。
    使用结构化字段（metadata.is_error, author.role, content.content_type）判断，
    而非依赖易变的文本匹配。

    参数：
    - `backend`：OpenAIBackendAPI 实例。
    - `conversation_id`：会话 ID。
    - `timeout_secs`：请求超时秒数。
    - `wait_secs`：等待任务创建的秒数。设为 0 可跳过等待。

    返回：
    - 详细错误信息文本，如果未找到则返回空字符串。
    """
    import time as _time
    try:
        if wait_secs > 0:
            _time.sleep(wait_secs)
        tasks = backend._query_backend_tasks(conversation_id=conversation_id, timeout_secs=timeout_secs)
        if not tasks:
            return ""

        for task in tasks:
            is_error, error_msg, metadata = backend.check_task_error(task)
            if is_error and error_msg:
                logger.info({
                    "event": "image_task_structured_error",
                    "conversation_id": conversation_id,
                    "error_msg": error_msg,
                    "metadata": metadata,
                })
                return error_msg
        return ""
    except Exception as exc:
        logger.warning({
            "event": "image_task_error_query_failed",
            "conversation_id": conversation_id,
            "error": str(exc),
        })
        return ""


def _remove_image_conversation_later(
        backend: OpenAIBackendAPI,
        conversation_id: str,
        *,
        success: bool,
) -> None:
    if not conversation_id:
        return
    if not (config.image_remove_conversation_always or (success and config.image_remove_conversation_after_result)):
        return

    def _run() -> None:
        try:
            backend.delete_conversation(conversation_id)
            logger.info({"event": "image_conversation_removed", "conversation_id": conversation_id})
        except Exception as exc:
            logger.warning({
                "event": "image_conversation_remove_failed",
                "conversation_id": conversation_id,
                "error": str(exc),
            })

    threading.Thread(target=_run, name=f"remove-image-conversation-{conversation_id}", daemon=True).start()


def stream_image_outputs(
        backend: OpenAIBackendAPI,
        request: ConversationRequest,
        index: int = 1,
        total: int = 1,
) -> Iterator[ImageOutput]:
    last: dict[str, Any] = {}
    reported_conversation_id = ""
    for event in conversation_events(
            backend,
            prompt=request.prompt,
            model=request.model,
            images=request.images or [],
            size=request.size,
            quality=request.quality,
            comments=request.comments,
            markup=request.markup,
    ):
        last = event
        event_conversation_id = str(event.get("conversation_id") or "")
        if event_conversation_id and event_conversation_id != reported_conversation_id:
            _report_image_progress(request, "image_conversation_id", event_conversation_id)
            reported_conversation_id = event_conversation_id
        if event.get("type") == "conversation.delta":
            yield ImageOutput(
                kind="progress",
                model=request.model,
                index=index,
                total=total,
                text=str(event.get("delta") or ""),
                upstream_event_type="conversation.delta",
                conversation_id=str(event.get("conversation_id") or ""),
            )
            continue
        if event.get("type") == "conversation.event":
            raw = event.get("raw")
            raw_type = str(raw.get("type") or "") if isinstance(raw, dict) else ""
            yield ImageOutput(
                kind="progress",
                model=request.model,
                index=index,
                total=total,
                upstream_event_type=raw_type,
                conversation_id=str(event.get("conversation_id") or ""),
            )

    conversation_id = str(last.get("conversation_id") or "")
    file_ids = [str(item) for item in last.get("file_ids") or []]
    sediment_ids = [str(item) for item in last.get("sediment_ids") or []]
    message = str(last.get("text") or "").strip()
    logger.info({
        "event": "image_stream_resolve_start",
        "conversation_id": conversation_id,
        "file_ids": file_ids,
        "sediment_ids": sediment_ids,
        "tool_invoked": last.get("tool_invoked"),
        "turn_use_case": last.get("turn_use_case"),
    })
    _report_image_progress(request, "image_stream_resolve_start", conversation_id)
    # 滑动窗口限流快速失败：tool 消息已返回 "generating images too quickly"，
    # 上游不会再产出图片，继续走 tasks/poll 只会白等 image_poll_timeout_secs。
    # 抛出专用异常交由 _generate_single_image 做账号冷却 + 换号重试。
    if not file_ids and not sediment_ids and last.get("rate_limited"):
        logger.warning({
            "event": "image_stream_rate_limited",
            "conversation_id": conversation_id,
            "message": message[:200],
        })
        raise ImageRateLimitedError(
            "You're generating images too quickly. To ensure the best experience "
            "for everyone, we have rate limits in place.",
            conversation_id=conversation_id,
            retry_after_secs=float(config.image_rate_limit_cooldown_secs),
        )
    if message and not file_ids and not sediment_ids and last.get("blocked"):
        # 尝试从 /backend-api/tasks/ 获取详细错误信息
        detailed_error = _get_detailed_error_from_tasks(backend, conversation_id)
        error_text = detailed_error or message or "Image generation was rejected by upstream policy."
        yield ImageOutput(kind="message", model=request.model, index=index, total=total, text=error_text, conversation_id=conversation_id)
        return
    should_poll_for_image = bool(request.images) or last.get("turn_use_case") == "image gen"
    if message and not file_ids and not sediment_ids and not should_poll_for_image:
        yield ImageOutput(kind="message", model=request.model, index=index, total=total, text=message, conversation_id=conversation_id)
        return

    # 检测模型是否返回了文本描述（含 referenced_image_ids）而非实际生成图片
    # 这说明模型已发起图片生成工具调用，但 SSE 在工具完成前断开，
    # 图片可能正在异步生成中。需要使用更积极的轮询策略来获取结果。
    is_text_reply = bool(message and is_model_text_reply_instead_of_image(message))
    if is_text_reply:
        logger.info({
            "event": "image_detected_text_reply_with_ids",
            "conversation_id": conversation_id,
            "message_preview": message[:200],
        })

    # 当检测到文本回复但 conversation_id 丢失时，尝试从最近对话列表中恢复
    # SSE 流太短时（模型返回文本而非触发图片工具），conversation_id 可能未被捕获，
    # 但图片已在上游异步生成。通过列出最近对话来恢复 conversation_id。
    if is_text_reply and not conversation_id:
        try:
            import time as _time
            recovered_id = backend.find_conversation_by_prompt(
                request.prompt, _time.time(), timeout_secs=5.0,
            )
            if recovered_id:
                conversation_id = recovered_id
                _report_image_progress(request, "image_conversation_id", conversation_id)
                logger.info({
                    "event": "image_conversation_id_recovered",
                    "conversation_id": conversation_id,
                    "message_preview": message[:200],
                })
        except Exception as exc:
            logger.warning({
                "event": "image_conversation_id_recovery_failed",
                "error": repr(exc)[:300],
            })

    # 在轮询图片之前，先检查 /backend-api/tasks/ 是否有 moderation 拦截
    # 这样可以避免不必要的长时间轮询超时
    # 注意：当 should_poll_for_image 为 True 或检测到文本回复时，
    # 即使 tasks 报告了"错误"，也不能直接返回——因为上游可能将工具调用的 JSON 参数
    # （如 {"size":"1792x1024","n":1}）标记为 is_error，而实际上图片正在异步生成中。
    # 此时应继续轮询图片。
    detailed_error = ""
    if not file_ids and not sediment_ids and conversation_id:
        detailed_error = _get_detailed_error_from_tasks(backend, conversation_id, timeout_secs=5.0, wait_secs=1.0)
        if detailed_error and not should_poll_for_image and not is_text_reply:
            logger.info({
                "event": "image_task_error_before_poll",
                "conversation_id": conversation_id,
                "error": detailed_error,
            })
            yield ImageOutput(kind="message", model=request.model, index=index, total=total, text=detailed_error, conversation_id=conversation_id)
            return
        if detailed_error and (should_poll_for_image or is_text_reply):
            logger.info({
                "event": "image_task_error_skipped_for_poll",
                "conversation_id": conversation_id,
                "error": detailed_error,
            })

    # 当检测到文本回复（含 referenced_image_ids）时，使用更长的超时来轮询图片结果。
    # 因为上游可能将图片生成作为异步任务执行，SSE 流在工具完成前就断开了，
    # 导致对话文档中尚未写入图片工具的响应记录。
    poll_timeout = config.image_poll_timeout_secs
    if is_text_reply and conversation_id:
        # 文本回复场景下图片可能仍在异步生成，使用更长超时（默认 120s → 额外 180s = 300s）
        poll_timeout = max(poll_timeout, 300)
        logger.info({
            "event": "image_text_reply_extended_poll",
            "conversation_id": conversation_id,
            "poll_timeout_secs": poll_timeout,
        })

    try:
        image_urls = backend.resolve_conversation_image_urls(
            conversation_id, file_ids, sediment_ids, poll_timeout_secs=poll_timeout,
        )
    except (ImageContentPolicyError, ImagePollTimeoutError) as exc:
        # 当检测到文本回复时，task error 不应直接判定为内容策略违规，
        # 因为图片可能仍在后台异步生成中
        if is_text_reply and isinstance(exc, ImageContentPolicyError):
            logger.warning({
                "event": "image_text_reply_task_error_ignored",
                "conversation_id": conversation_id,
                "error": str(exc),
            })
            image_urls = []
        else:
            raise
    except Exception as exc:
        # 当检测到文本回复时，首次轮询的临时网络错误不应直接中断，
        # 因为图片可能仍在后台异步生成中，后续 retry poll 会继续尝试。
        if isinstance(exc, ImageRateLimitedError):
            raise
        if is_text_reply and conversation_id:
            logger.warning({
                "event": "image_text_reply_first_poll_error_ignored",
                "conversation_id": conversation_id,
                "error": repr(exc)[:300],
            })
            image_urls = []
        else:
            raise

    if image_urls:
        if request.progress_callback:
            request.progress_callback("receiving_image")
        image_items = image_result_items_from_urls(backend, image_urls, request.response_format)
        data = format_image_result(
            image_items,
            request.prompt,
            request.response_format,
            int(time.time()),
        )["data"]
        if data:
            yield ImageOutput(kind="result", model=request.model, index=index, total=total, data=data, conversation_id=conversation_id)
        return

    if message:
        # 检测模型是否返回了文本描述（含 referenced_image_ids）而非实际生成图片
        # 这说明模型已发起图片生成工具调用，但 SSE 在工具完成前断开。
        # 此时应再尝试轮询图片结果，而不是直接把文本当作最终输出。
        # 当 is_text_reply 但 conversation_id 丢失时，尝试从最近对话列表恢复
        if is_text_reply and not conversation_id:
            try:
                import time as _time
                recovered_id = backend.find_conversation_by_prompt(
                    request.prompt, _time.time(), timeout_secs=5.0,
                )
                if recovered_id:
                    conversation_id = recovered_id
                    _report_image_progress(request, "image_conversation_id", conversation_id)
                    logger.info({
                        "event": "image_text_reply_conversation_id_recovered",
                        "conversation_id": conversation_id,
                        "message_preview": message[:200],
                    })
            except Exception as exc:
                logger.warning({
                    "event": "image_text_reply_conversation_id_recovery_failed",
                    "error": repr(exc)[:300],
                })
        if is_text_reply and conversation_id:
            logger.info({
                "event": "image_model_text_reply_retry_poll",
                "conversation_id": conversation_id,
                "message_preview": message[:200],
            })
            # 文本回复场景下，图片可能需要 4-5 分钟才能异步生成完成。
            # 使用 300s 超时并允许多次重试，避免因临时网络问题提前退出。
            retry_poll_timeout = max(config.image_poll_timeout_secs, 300)
            MAX_POLL_RETRIES = 3
            for poll_attempt in range(1, MAX_POLL_RETRIES + 1):
                try:
                    polled_file_ids, polled_sediment_ids = backend._poll_image_results(
                        conversation_id,
                        retry_poll_timeout,
                        file_ids,
                        sediment_ids,
                    )
                    file_ids.extend(item for item in polled_file_ids if item and item not in file_ids)
                    sediment_ids.extend(item for item in polled_sediment_ids if item and item not in sediment_ids)
                    break  # 轮询成功，退出重试循环
                except Exception as exc:
                    if isinstance(exc, ImageRateLimitedError):
                        raise
                    error_str = str(exc)
                    is_transient = (
                        isinstance(exc, ImagePollTimeoutError)
                        or is_tls_connection_error(error_str)
                        or "upstream" in error_str.lower()
                        or "connection" in error_str.lower()
                        or "timeout" in error_str.lower()
                    )
                    logger.warning({
                        "event": "image_model_text_reply_poll_failed",
                        "conversation_id": conversation_id,
                        "poll_attempt": poll_attempt,
                        "error": repr(exc)[:300],
                        "is_transient": is_transient,
                    })
                    # 如果还有重试次数且不是超时/内容违规错误，继续重试
                    if poll_attempt < MAX_POLL_RETRIES and not isinstance(exc, (ImagePollTimeoutError, ImageContentPolicyError)):
                        # 递增退避：30s, 60s, 90s
                        backoff = 30.0 * poll_attempt
                        logger.info({
                            "event": "image_model_text_reply_poll_retry",
                            "conversation_id": conversation_id,
                            "poll_attempt": poll_attempt,
                            "backoff_secs": backoff,
                        })
                        time.sleep(backoff)
                        continue
                    # 超时错误或重试次数用尽，停止重试
                    break

            if file_ids or sediment_ids:
                image_urls = backend.resolve_conversation_image_urls(
                    conversation_id, file_ids, sediment_ids, poll=False,
                )
                if image_urls:
                    if request.progress_callback:
                        request.progress_callback("receiving_image")
                    image_items = image_result_items_from_urls(backend, image_urls, request.response_format)
                    data = format_image_result(
                        image_items,
                        request.prompt,
                        request.response_format,
                        int(time.time()),
                    )["data"]
                    if data:
                        yield ImageOutput(kind="result", model=request.model, index=index, total=total, data=data, conversation_id=conversation_id)
                        return
        elif is_text_reply:
            logger.warning({
                "event": "image_model_text_reply_no_image",
                "conversation_id": conversation_id,
                "message_preview": message[:200],
            })
        yield ImageOutput(kind="message", model=request.model, index=index, total=total, text=message, conversation_id=conversation_id)
        return

    # 兜底：当 message 为空且图片 URL 解析失败时，先尝试一次短延迟重试轮询
    # 然后抛出明确错误而非让调用方得到 "upstream completed without generating images" 这种模糊报错
    logger.warning({
        "event": "image_stream_no_result_fallback",
        "conversation_id": conversation_id,
        "file_ids": file_ids,
        "sediment_ids": sediment_ids,
        "should_poll_for_image": should_poll_for_image,
    })
    # 当 should_poll_for_image 为 True 但 conversation_id 丢失时，尝试恢复
    if should_poll_for_image and not conversation_id:
        try:
            import time as _time
            recovered_id = backend.find_conversation_by_prompt(
                request.prompt, _time.time(), timeout_secs=5.0,
            )
            if recovered_id:
                conversation_id = recovered_id
                _report_image_progress(request, "image_conversation_id", conversation_id)
                logger.info({
                    "event": "image_fallback_conversation_id_recovered",
                    "conversation_id": conversation_id,
                })
        except Exception as exc:
            logger.warning({
                "event": "image_fallback_conversation_id_recovery_failed",
                "error": repr(exc)[:300],
            })
    if should_poll_for_image and conversation_id:
        # 图片可能仍在异步处理中（上游 SSE 流在图片生成完成前就结束了）。
        # 使用 300s 超时并允许多次重试，避免因临时网络问题或图片尚未提交而提前退出。
        retry_poll_timeout = max(config.image_poll_timeout_secs, 300)
        MAX_FALLBACK_POLL_RETRIES = 3
        for poll_attempt in range(1, MAX_FALLBACK_POLL_RETRIES + 1):
            retry_wait_secs = min(30.0 * poll_attempt, config.image_poll_initial_wait_secs * poll_attempt)
            logger.info({
                "event": "image_stream_retry_poll_after_wait",
                "conversation_id": conversation_id,
                "retry_wait_secs": retry_wait_secs,
                "poll_attempt": poll_attempt,
            })
            time.sleep(retry_wait_secs)
            try:
                polled_file_ids, polled_sediment_ids = backend._poll_image_results(
                    conversation_id,
                    retry_poll_timeout,
                    file_ids,
                    sediment_ids,
                )
                file_ids.extend(item for item in polled_file_ids if item and item not in file_ids)
                sediment_ids.extend(item for item in polled_sediment_ids if item and item not in sediment_ids)
                break  # 轮询成功，退出重试循环
            except Exception as exc:
                if isinstance(exc, ImageRateLimitedError):
                    raise
                error_str = str(exc)
                is_transient = (
                    isinstance(exc, ImagePollTimeoutError)
                    or is_tls_connection_error(error_str)
                    or "upstream" in error_str.lower()
                    or "connection" in error_str.lower()
                    or "timeout" in error_str.lower()
                )
                logger.warning({
                    "event": "image_stream_retry_poll_failed",
                    "conversation_id": conversation_id,
                    "poll_attempt": poll_attempt,
                    "error": repr(exc)[:300],
                    "is_transient": is_transient,
                })
                # 如果还有重试次数且不是超时/内容违规错误，继续重试
                if poll_attempt < MAX_FALLBACK_POLL_RETRIES and not isinstance(exc, (ImagePollTimeoutError, ImageContentPolicyError)):
                    # 递增退避：30s, 60s
                    backoff = 30.0 * poll_attempt
                    logger.info({
                        "event": "image_stream_retry_poll_retry",
                        "conversation_id": conversation_id,
                        "poll_attempt": poll_attempt,
                        "backoff_secs": backoff,
                    })
                    time.sleep(backoff)
                    continue
                # 超时错误或重试次数用尽，停止重试
                break
        
        if file_ids or sediment_ids:
            image_urls = backend.resolve_conversation_image_urls(
                conversation_id, file_ids, sediment_ids, poll=False,
            )
            if image_urls:
                if request.progress_callback:
                    request.progress_callback("receiving_image")
                image_items = image_result_items_from_urls(backend, image_urls, request.response_format)
                data = format_image_result(
                    image_items,
                    request.prompt,
                    request.response_format,
                    int(time.time()),
                )["data"]
                if data:
                    yield ImageOutput(kind="result", model=request.model, index=index, total=total, data=data, conversation_id=conversation_id)
                    return
        
        # 重试后仍然失败，yield 错误消息
        yield ImageOutput(kind="message", model=request.model, index=index, total=total,
                          text="Image generation completed upstream but the result could not be retrieved. "
                               "The image may still be processing. Please try again in a moment.",
                          conversation_id=conversation_id)
    elif message:
        yield ImageOutput(kind="message", model=request.model, index=index, total=total, text=message, conversation_id=conversation_id)
    else:
        # conversation_id 也为空时（SSE 流极短、未捕获到会话 ID），
        # 仍然 yield 一条消息，避免 stream_image_outputs_with_pool 产生
        # "upstream completed without generating images" 模糊报错
        yield ImageOutput(kind="message", model=request.model, index=index, total=total,
                          text="Image generation started upstream but the response was incomplete. "
                               "Please try again.",
                          conversation_id=conversation_id)


def _codex_response_images(value: Any) -> list[str]:
    if isinstance(value, dict):
        if value.get("type") == "image_generation_call" and isinstance(value.get("result"), str):
            result = value["result"].strip()
            if result:
                return [result.split(",", 1)[1] if result.startswith("data:image/") else result]
        images: list[str] = []
        for item in value.values():
            images.extend(_codex_response_images(item))
        return images
    if isinstance(value, list):
        images: list[str] = []
        for item in value:
            images.extend(_codex_response_images(item))
        return images
    return []


def stream_codex_image_outputs(
        backend: OpenAIBackendAPI,
        request: ConversationRequest,
        index: int = 1,
        total: int = 1,
) -> Iterator[ImageOutput]:
    images: list[str] = []
    for event in backend.iter_codex_image_response_events(
        prompt=request.prompt,
        images=request.images or [],
        size=request.size,
        quality=request.quality,
    ):
        for image in _codex_response_images(event):
            if image not in images:
                images.append(image)
    if not images:
        raise ImageGenerationError("No image result found in response")
    data = format_image_result(
        [{"b64_json": item, "revised_prompt": request.prompt} for item in images],
        request.prompt,
        request.response_format,
        int(time.time()),
    )["data"]
    if data:
        yield ImageOutput(kind="result", model=request.model, index=index, total=total, data=data)
        return
    raise ImageGenerationError("No image result found in response")


def _generate_single_image(
        request: ConversationRequest,
        index: int,
        total: int,
) -> list[ImageOutput]:
    """为单张图片执行生成逻辑（含重试），返回结果列表。

    The caller owns the worker and account lease; this function only handles
    one output and its bounded retry decisions.
    """
    # 模型返回文本而非图片的最大重试次数
    MAX_TEXT_REPLY_RETRIES = 3
    # TLS 连接错误最大重试次数
    MAX_TLS_RETRIES = 3
    # 连接超时错误最大重试次数（同账号短等待重试）
    MAX_CONN_TIMEOUT_RETRIES = 3
    # 轮询超时错误最大重试次数（换账号重试）
    MAX_POLL_TIMEOUT_RETRIES = 4
    # 滑动窗口限流最大换号重试次数：命中限流的账号已进入冷却，
    # 换号必定选到不同账号；重试上限取配置值，超出后返回 429。
    MAX_RATE_LIMIT_RETRIES = max(0, int(config.image_rate_limit_failover_retries))

    text_reply_retry_count = 0
    tls_retry_count = 0
    conn_timeout_retry_count = 0
    poll_timeout_retry_count = 0
    rate_limit_retry_count = 0
    account_email = ""
    last_rate_limit_retry_after = float(config.image_rate_limit_cooldown_secs)

    def _cooldown_account_for_rate_limit(failed_token: str) -> None:
        """命中滑动窗口限流后，把该账号的图片通道冷却 image_rate_limit_cooldown_secs。"""
        nonlocal last_rate_limit_retry_after
        cooldown_secs = float(config.image_rate_limit_cooldown_secs)
        last_rate_limit_retry_after = cooldown_secs
        context = current_execution_context()
        if context is not None and context.lease is not None:
            try:
                from services.scheduler.service import scheduler_service as _scheduler_service

                lanes = {Lane.IMAGE_GENERATION, Lane.IMAGE_EDIT, context.lease.lane}
                for lane in lanes:
                    _scheduler_service.account_pool.open_breaker(
                        context.lease.account_key, lane, cooldown_secs,
                    )
                logger.warning({
                    "event": "image_rate_limit_cooldown",
                    "account_ref": anonymize_token(failed_token),
                    "account_email": account_email,
                    "account_key": context.lease.account_key,
                    "cooldown_secs": cooldown_secs,
                    "lanes": sorted(str(lane.value) for lane in lanes),
                })
                return
            except Exception as exc:
                logger.warning({
                    "event": "image_rate_limit_cooldown_breaker_failed",
                    "account_ref": anonymize_token(failed_token),
                    "error": repr(exc)[:200],
                })
        account_service.mark_image_rate_limited(failed_token, cooldown_secs)
        logger.warning({
            "event": "image_rate_limit_cooldown",
            "account_ref": anonymize_token(failed_token),
            "account_email": account_email,
            "cooldown_secs": cooldown_secs,
            "lane": "legacy_account_pool",
        })

    def select_token(excluded_tokens: set[str] | None = None) -> str:
        context = current_execution_context()
        if context is not None:
            if context.lease is None:
                raise RuntimeError("scheduler lease replacement unavailable")
            return context.lease.access_token
        plan_type, _ = split_image_model(request.model)
        codex_model = is_codex_image_model(request.model)
        return account_service.get_available_access_token(
            plan_type=plan_type,
            source_type="codex" if codex_model else None,
            plan_types=("plus", "team", "pro") if codex_model and not plan_type else None,
        )

    def mark_result(token: str, success: bool) -> None:
        context = current_execution_context()
        account_service.mark_image_result(
            token,
            success,
            release_slot=context is None,
        )

    def replace_lease(token: str, outcome: Outcome) -> str | None:
        context = current_execution_context()
        if context is None or context.replace_lease is None:
            return None
        replacement = context.replace_lease(context, {token}, outcome)
        return replacement.access_token if replacement is not None else None

    def replace_or_abort(token: str, outcome: Outcome) -> None:
        if replace_lease(token, outcome) is not None:
            return
        if current_execution_context() is not None:
            raise ImageGenerationError(
                "no replacement account is available",
                status_code=503,
                error_type="server_error",
                code="no_replacement_account",
                account_email=account_email,
            )

    while True:
        try:
            if request.progress_callback:
                request.progress_callback("getting_account")
            token = select_token()
        except RuntimeError as exc:
            # 选不到可用账号：号池为空或全部处于冷却（如滑动窗口限流），返回 429
            message_text = str(exc) or "image generation failed"
            if "no available" in message_text.lower():
                raise ImageGenerationError(
                    message_text,
                    status_code=429,
                    error_type="rate_limit_error",
                    code="no_available_image_account",
                    account_email=account_email,
                    retry_after_secs=max(1, int(config.image_rate_limit_cooldown_secs)),
                ) from exc
            raise ImageGenerationError(message_text, account_email=account_email) from exc

        emitted_for_token = False
        returned_message = False
        returned_result = False
        account = account_service.get_account(token) or {}
        account_email = str(account.get("email") or "").strip()
        logger.debug({
            "event": "image_account_lookup",
            "account_ref": anonymize_token(token),
            "account_email": account_email,
            "account_found": bool(account),
            "index": index,
        })
        backend = None
        try:
            backend = OpenAIBackendAPI(access_token=token)
            if request.progress_callback:
                backend.progress_callback = request.progress_callback
            stream_fn = stream_codex_image_outputs if is_codex_image_model(request.model) else stream_image_outputs
            outputs: list[ImageOutput] = []
            last_conversation_id = ""
            try:
                for output in stream_fn(backend, request, index, total):
                    last_conversation_id = output.conversation_id or last_conversation_id
                    if output.conversation_id:
                        _report_image_progress(request, "image_conversation_id", output.conversation_id)
                    if account_email and not output.account_email:
                        output.account_email = account_email
                    if output.kind == "message" and request.message_as_error:
                        raise ImageGenerationError(
                            output.text or "Image generation was rejected by upstream policy.",
                            status_code=400,
                            error_type="invalid_request_error",
                            code="content_policy_violation",
                            account_email=account_email,
                            conversation_id=output.conversation_id,
                        )
                    emitted_for_token = True
                    returned_message = output.kind == "message"
                    returned_result = returned_result or output.kind == "result"
                    outputs.append(output)
            except Exception as exc:
                # 异常路径（内容政策拒绝、轮询超时等）会话 ID 只挂在异常上
                last_conversation_id = last_conversation_id or str(getattr(exc, "conversation_id", "") or "")
                if last_conversation_id:
                    _report_image_progress(request, "image_conversation_id", last_conversation_id)
                    if not getattr(exc, "conversation_id", ""):
                        if isinstance(exc, (ImageGenerationError, ImageRateLimitedError)):
                            exc.conversation_id = last_conversation_id
                        else:
                            raise ImageGenerationError(
                                image_stream_error_message(str(exc)),
                                account_email=account_email,
                                conversation_id=last_conversation_id,
                            ) from exc
                raise
            finally:
                _remove_image_conversation_later(backend, last_conversation_id, success=returned_result)
            if returned_message:
                mark_result(token, False)
                return outputs
            if not returned_result:
                mark_result(token, False)
                if emitted_for_token:
                    conv_id = outputs[-1].conversation_id if outputs else ""
                    raise ImageGenerationError(
                        "upstream completed without generating images",
                        status_code=400,
                        error_type="invalid_request_error",
                        code="no_image_generated",
                        account_email=account_email,
                        conversation_id=conv_id,
                    )
                return outputs
            mark_result(token, True)
            return outputs
        except ImagePollTimeoutError as exc:
            mark_result(token, False)
            if account_email:
                setattr(exc, "account_email", account_email)
            remote_conversation_id = str(getattr(exc, "conversation_id", "") or "")
            if remote_conversation_id:
                # The upstream task was already accepted. Switching accounts
                # here would submit a duplicate generation; async task callers
                # can resume polling this conversation ID instead.
                raise ImageGenerationError(
                    str(exc) or "image generation polling timed out",
                    status_code=504,
                    error_type="server_error",
                    code="image_poll_timeout",
                    account_email=account_email,
                    conversation_id=remote_conversation_id,
                ) from exc
            # 轮询超时：换账号重试
            if not emitted_for_token:
                poll_timeout_retry_count += 1
                if poll_timeout_retry_count <= MAX_POLL_TIMEOUT_RETRIES:
                    logger.warning({
                        "event": "image_poll_timeout_retry",
                        "account_ref": anonymize_token(token),
                        "account_email": account_email,
                        "retry_count": poll_timeout_retry_count,
                        "index": index,
                        "error": str(exc)[:200],
                    })
                    replace_or_abort(token, Outcome.NETWORK_ERROR)
                    continue
                logger.warning({
                    "event": "image_poll_timeout_exhausted_retries",
                    "account_ref": anonymize_token(token),
                    "account_email": account_email,
                    "retry_count": poll_timeout_retry_count,
                    "index": index,
                })
                raise
            raise
        except ImageContentPolicyError as exc:
            mark_result(token, False)
            logger.warning({
                "event": "image_stream_content_policy_error",
                "account_ref": anonymize_token(token),
                "account_email": account_email,
                "error": str(exc),
                "index": index,
            })
            raise ImageGenerationError(
                str(exc) or "Image generation was rejected by upstream policy.",
                status_code=400,
                error_type="invalid_request_error",
                code="content_policy_violation",
                account_email=account_email,
                conversation_id=getattr(exc, "conversation_id", ""),
            ) from exc
        except ImageRateLimitedError as exc:
            # 滑动窗口限流：该账号短窗口内额度耗尽，冷却后换号重试。
            # 上游限流按账号计（不同账号独立额度），换号通常能立即成功；
            # 检测在 SSE 流内完成（秒级），换号成本远低于等满轮询超时。
            mark_result(token, False)
            _cooldown_account_for_rate_limit(token)
            if account_email:
                setattr(exc, "account_email", account_email)
            remote_conversation_id = str(getattr(exc, "conversation_id", "") or "")
            if rate_limit_retry_count < MAX_RATE_LIMIT_RETRIES:
                rate_limit_retry_count += 1
                logger.warning({
                    "event": "image_rate_limited_failover",
                    "account_ref": anonymize_token(token),
                    "account_email": account_email,
                    "retry_count": rate_limit_retry_count,
                    "max_retries": MAX_RATE_LIMIT_RETRIES,
                    "index": index,
                    "conversation_id": remote_conversation_id,
                })
                cooldown_secs = max(1, int(last_rate_limit_retry_after))
                try:
                    replace_or_abort(token, Outcome.RATE_LIMIT)
                except ImageGenerationError as replace_exc:
                    # 调度器已无可替换账号（其余账号同样在冷却/不可用），
                    # 与重试次数用尽等价：对下游返回 429 + Retry-After。
                    if getattr(replace_exc, "code", "") != "no_replacement_account":
                        raise
                    raise ImageGenerationError(
                        "Image generation is temporarily rate limited: all image accounts "
                        "are cooling down (upstream: generating images too quickly). "
                        f"Please retry in about {cooldown_secs // 60} minutes.",
                        status_code=429,
                        error_type="rate_limit_error",
                        code="image_rate_limited",
                        param=None,
                        account_email=account_email,
                        conversation_id=remote_conversation_id,
                        retry_after_secs=cooldown_secs,
                    ) from replace_exc
                continue
            logger.warning({
                "event": "image_rate_limited_exhausted_retries",
                "account_ref": anonymize_token(token),
                "account_email": account_email,
                "retry_count": rate_limit_retry_count,
                "index": index,
            })
            cooldown_secs = max(1, int(last_rate_limit_retry_after))
            raise ImageGenerationError(
                "Image generation is temporarily rate limited "
                "(upstream: generating images too quickly). "
                f"Cooling down affected accounts for about {cooldown_secs // 60} minutes; "
                "please retry later.",
                status_code=429,
                error_type="rate_limit_error",
                code="image_rate_limited",
                param=None,
                account_email=account_email,
                conversation_id=remote_conversation_id,
                retry_after_secs=cooldown_secs,
            ) from exc
        except ImageGenerationError as exc:
            mark_result(token, False)
            if account_email and not getattr(exc, "account_email", ""):
                exc.account_email = account_email
            error_text = str(exc)
            # 如果是模型返回文本而非图片，尝试换账号重试
            remote_conversation_id = str(getattr(exc, "conversation_id", "") or "")
            if is_model_text_reply_instead_of_image(error_text) and not emitted_for_token and not remote_conversation_id:
                text_reply_retry_count += 1
                if text_reply_retry_count <= MAX_TEXT_REPLY_RETRIES:
                    logger.warning({
                        "event": "image_model_text_reply_retry",
                        "account_ref": anonymize_token(token),
                        "account_email": account_email,
                        "retry_count": text_reply_retry_count,
                        "index": index,
                        "error": error_text[:200],
                    })
                    replace_or_abort(token, Outcome.SERVER_ERROR)
                    continue
                logger.warning({
                    "event": "image_model_text_reply_exhausted_retries",
                    "account_ref": anonymize_token(token),
                    "account_email": account_email,
                    "retry_count": text_reply_retry_count,
                    "index": index,
                })
                raise ImageGenerationError(
                    "Image generation failed: the upstream model returned a text description "
                    "instead of generating an image. Please try again later.",
                    status_code=502,
                    error_type="server_error",
                    code="upstream_text_reply",
                    account_email=account_email,
                    conversation_id=getattr(exc, "conversation_id", ""),
                ) from exc
            logger.warning({
                "event": "image_stream_generation_error",
                "account_ref": anonymize_token(token),
                "account_email": account_email,
                "error": error_text,
                "index": index,
            })
            raise
        except Exception as exc:
            mark_result(token, False)
            last_error = str(exc)
            logger.warning({
                "event": "image_stream_fail",
                "account_ref": anonymize_token(token),
                "account_email": account_email,
                "error": last_error,
                "index": index,
            })
            remote_conversation_id = str(getattr(exc, "conversation_id", "") or "")
            if remote_conversation_id:
                _report_image_progress(request, "image_conversation_id", remote_conversation_id)
                raise ImageGenerationError(
                    image_stream_error_message(last_error),
                    status_code=504,
                    error_type="server_error",
                    code="image_poll_timeout",
                    account_email=account_email,
                    conversation_id=remote_conversation_id,
                ) from exc
            if not emitted_for_token and is_token_invalid_error(last_error):
                had_replacement = replace_lease(token, Outcome.AUTH_ERROR) is not None
                if had_replacement:
                    continue
                if current_execution_context() is not None:
                    raise ImageGenerationError(
                        "no replacement account is available",
                        status_code=503,
                        error_type="server_error",
                        code="no_replacement_account",
                        account_email=account_email,
                    ) from exc
                refreshed_token = account_service.refresh_access_token(token, force=True, event="image_stream")
                if refreshed_token and refreshed_token != token:
                    token = refreshed_token
                    continue
                account_service.remove_invalid_token(token, "image_stream")
                continue
            # TLS/SSL 连接错误：自动重试
            if not emitted_for_token and is_tls_connection_error(last_error):
                tls_retry_count += 1
                if tls_retry_count <= MAX_TLS_RETRIES:
                    logger.warning({
                        "event": "image_stream_tls_retry",
                        "account_ref": anonymize_token(token),
                        "account_email": account_email,
                        "retry_count": tls_retry_count,
                        "index": index,
                        "error": last_error[:200],
                    })
                    time.sleep(min(2.0 * tls_retry_count, 10.0))
                    replace_or_abort(token, Outcome.NETWORK_ERROR)
                    continue
            # 连接超时错误（curl 28）：同账号短等待重试，不切换账号
            if not emitted_for_token and is_connection_timeout_error(last_error):
                conn_timeout_retry_count += 1
                if conn_timeout_retry_count <= MAX_CONN_TIMEOUT_RETRIES:
                    wait_secs = min(3.0 * conn_timeout_retry_count, 9.0)
                    logger.warning({
                        "event": "image_stream_conn_timeout_retry",
                        "account_ref": anonymize_token(token),
                        "account_email": account_email,
                        "retry_count": conn_timeout_retry_count,
                        "index": index,
                        "wait_secs": wait_secs,
                        "error": last_error[:200],
                    })
                    time.sleep(wait_secs)
                    replace_or_abort(token, Outcome.NETWORK_ERROR)
                    continue
            raise ImageGenerationError(image_stream_error_message(last_error), account_email=account_email, conversation_id="") from exc
        finally:
            if backend is not None:
                backend.close()


def generate_image_unit(request: ConversationRequest, index: int, total: int) -> list[ImageOutput]:
    """Generate one output while the scheduler owns the surrounding lease."""
    from services import sub2_gateway
    if sub2_gateway.enabled():
        from dataclasses import replace
        return list(stream_image_outputs_with_pool(replace(request, n=1)))
    return _generate_single_image(request, index, total)


def stream_image_outputs_with_pool(request: ConversationRequest) -> Iterator[ImageOutput]:
    """Generate requested outputs through the caller's bounded scheduler lane."""
    from services.protocol import image_upstream
    if image_upstream.configured():
        body = {"model": request.model, "prompt": request.prompt, "n": request.n,
                "size": request.size, "quality": request.quality, "response_format": request.response_format}
        yield from image_upstream.edit(body, request) if request.images else image_upstream.generate(body)
        return
    if not is_supported_image_model(request.model):
        raise ImageGenerationError("unsupported image model,supported models: " + ", ".join(sorted(IMAGE_MODELS)))

    results: dict[int, list[ImageOutput]] = {}
    errors: dict[int, Exception] = {}
    for index in range(1, max(1, request.n) + 1):
        try:
            results[index] = generate_image_unit(request, index, max(1, request.n))
        except Exception as exc:
            errors[index] = exc
            logger.warning({
                "event": "image_generation_unit_error",
                "index": index,
                "error": str(exc)[:300],
            })

    emitted = False
    last_error = ""
    first_error_exc: Exception | None = None
    for index in range(1, max(1, request.n) + 1):
        if index in results:
            for output in results[index]:
                emitted = True
                yield output
        elif index in errors:
            last_error = str(errors[index])
            if first_error_exc is None:
                first_error_exc = errors[index]

    if not emitted:
        # 保留已构造好的 ImageGenerationError（如限流 429、轮询超时 504）的
        # 状态码与错误码语义，仅对未知异常做统一包装。
        if isinstance(first_error_exc, ImageGenerationError):
            raise first_error_exc
        raise ImageGenerationError(
            image_stream_error_message(last_error or "no account in the pool could generate images"),
            conversation_id="",
        )


def stream_image_chunks(outputs: Iterable[ImageOutput]) -> Iterator[dict[str, Any]]:
    for output in outputs:
        yield output.to_chunk()


def collect_image_outputs(outputs: Iterable[ImageOutput]) -> dict[str, Any]:
    created = None
    data: list[dict[str, Any]] = []
    message = ""
    progress_parts: list[str] = []
    account_email = ""
    conversation_id = ""
    for output in outputs:
        created = created or output.created
        if output.account_email and not account_email:
            account_email = output.account_email
        if output.conversation_id:
            conversation_id = output.conversation_id
        if output.kind == "progress" and output.text:
            progress_parts.append(output.text)
        elif output.kind == "message":
            message = output.text
        elif output.kind == "result":
            data.extend(output.data)

    result: dict[str, Any] = {"created": created or int(time.time()), "data": data}
    if not data:
        text = message or "".join(progress_parts).strip()
        if text:
            result["message"] = text
    if account_email:
        result["_account_email"] = account_email
    if conversation_id:
        result["_conversation_id"] = conversation_id
    return result
