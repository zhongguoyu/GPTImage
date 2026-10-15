from __future__ import annotations

import asyncio

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from api.image_inputs import ImageInputBudget, parse_image_edit_request, read_image_sources
from api.support import require_identity, resolve_image_base_url, run_with_quota
from services.content_filter import check_request, request_shape, request_text
from services.config import config
from services.editable_file_task_service import editable_file_task_service
from services.log_service import LoggedCall
from services.model_service import model_catalog_service
from services.protocol import (
    anthropic_v1_messages,
    openai_v1_chat_complete,
    openai_v1_image_edit,
    openai_v1_image_generations,
    openai_v1_models,
    openai_v1_response,
    openai_search,
)
from services.protocol.conversation import generate_image_unit
from services.scheduler.models import Lane
from services.scheduler.errors import NoProviderAvailableError
from services.scheduler.service import scheduler_service
from utils.helper import extract_chat_image, is_image_chat_request

MAX_IMAGE_OUTPUTS = 4


def _scheduler_enabled() -> bool:
    from services import sub2_gateway
    if sub2_gateway.enabled():
        return False
    settings = getattr(scheduler_service, "settings", None)
    return bool(getattr(settings, "enabled", True))


class ImageGenerationRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    model: str = "gpt-image-2"
    n: int = Field(default=1, ge=1, le=4)
    size: str | None = None
    quality: str = "auto"
    response_format: str = "b64_json"
    history_disabled: bool = True
    stream: bool | None = None


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    model: str | None = None
    prompt: str | None = None
    n: int | None = None
    stream: bool | None = None
    modalities: list[str] | None = None
    messages: list[dict[str, object]] | None = None


class ResponseCreateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    model: str | None = None
    input: object | None = None
    tools: list[dict[str, object]] | None = None
    tool_choice: object | None = None
    stream: bool | None = None


class AnthropicMessageRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    model: str | None = None
    messages: list[dict[str, object]] | None = None
    system: object | None = None
    stream: bool | None = None


class SearchRequest(BaseModel):
    prompt: str = Field(..., min_length=1)


class EditableFileTaskRequest(BaseModel):
    prompt: str = ""
    base64_images: list[str] = Field(default_factory=list)
    client_task_id: str | None = None


async def filter_or_log(call: LoggedCall, text: str) -> None:
    try:
        await run_in_threadpool(check_request, text)
    except HTTPException as exc:
        call.log("调用失败", status="failed", error=str(exc.detail))
        raise


def _text_scheduler_runner(model: str):
    if not _scheduler_enabled():
        return None
    requirement = scheduler_service.classify_text(model)

    def allows_anonymous() -> bool:
        try:
            route = model_catalog_service.cached_route_for_model(model)
        except Exception:
            return False
        return bool(route is not None and route.allow_anonymous)

    async def runner(handler, *args, streaming: bool = False):
        work = lambda _lease, _cancel: handler(*args)
        try:
            if streaming:
                return await scheduler_service.run_stream(Lane.TEXT, requirement, work)
            return await scheduler_service.run_text(requirement, work)
        except NoProviderAvailableError:
            if not allows_anonymous():
                raise
            return await run_in_threadpool(handler, *args)

    return runner


def _requested_image_count(payload: dict[str, object]) -> int:
    try:
        requested_count = int(payload.get("n") or 0)
    except (TypeError, ValueError):
        requested_count = 0
    if requested_count < 1:
        for tool in payload.get("tools") or []:
            if isinstance(tool, dict) and tool.get("type") == "image_generation":
                try:
                    requested_count = int(tool.get("n") or 0)
                except (TypeError, ValueError):
                    requested_count = 0
                break
    return max(1, requested_count)


def _image_scheduler_runner(model: str, payload: dict[str, object], lane: Lane):
    from services.protocol.image_upstream import configured
    if configured():
        return None
    if not _scheduler_enabled():
        return None
    scheduler_payload = dict(payload)
    requested_count = _requested_image_count(scheduler_payload)
    if requested_count > MAX_IMAGE_OUTPUTS:
        raise HTTPException(
            status_code=400,
            detail={"error": f"n must be between 1 and {MAX_IMAGE_OUTPUTS}"},
        )
    scheduler_payload["n"] = requested_count
    requirement = (
        scheduler_service.classify_image_generation(scheduler_payload)
        if lane == Lane.IMAGE_GENERATION
        else scheduler_service.classify_image_edit(scheduler_payload)
    )
    fallback_body = {
        key: value
        for key, value in payload.items()
        if key not in {"messages", "input", "images", "mask"}
    }

    async def runner(handler, *args, streaming: bool = False):
        body = args[0] if args and isinstance(args[0], dict) else fallback_body
        adapter = _image_protocol_adapter(handler, body)
        if adapter is None:
            # Keep compatibility with tests and extension handlers that wrap
            # the built-in protocol function. Built-in production handlers
            # always use the work-unit path below.
            return await run_in_threadpool(handler, *args)

        build_request, format_outputs = adapter
        request = await run_in_threadpool(build_request, body)
        body = _compact_image_scheduler_body(handler, body)
        del args
        if streaming:
            futures = await scheduler_service.submit_image_units(
                requirement,
                lambda index: generate_image_unit(request, index, requirement.cost),
            )

            async def async_formatted():
                try:
                    for completed in asyncio.as_completed(futures):
                        result = await completed
                        outputs = result if isinstance(result, list) else [result]
                        formatted = await run_in_threadpool(
                            lambda: list(format_outputs(outputs, body))
                        )
                        for item in formatted:
                            yield item
                finally:
                    for future in futures:
                        if not future.done():
                            future.cancel()
                    await asyncio.gather(
                        *(asyncio.shield(future) for future in futures),
                        return_exceptions=True,
                    )

            return async_formatted()
        unit_results = await scheduler_service.run_image_units(
            requirement,
            lambda index: generate_image_unit(request, index, requirement.cost),
        )
        outputs = []
        for result in unit_results:
            if isinstance(result, list):
                outputs.extend(result)
            else:
                outputs.append(result)
        formatted = await run_in_threadpool(format_outputs, outputs, body)
        return formatted

    return runner


def _image_protocol_adapter(handler, body: dict[str, object]):
    module_name = getattr(handler, "__module__", "")
    adapters = {
        openai_v1_image_generations.__name__: (
            openai_v1_image_generations.build_request,
            openai_v1_image_generations.format_outputs,
        ),
        openai_v1_image_edit.__name__: (
            openai_v1_image_edit.build_request,
            openai_v1_image_edit.format_outputs,
        ),
        openai_v1_chat_complete.__name__: (
            openai_v1_chat_complete.build_image_request,
            openai_v1_chat_complete.format_image_outputs,
        ),
        openai_v1_response.__name__: (
            openai_v1_response.build_image_request,
            openai_v1_response.format_image_outputs,
        ),
    }
    return adapters.get(module_name)


def _compact_image_scheduler_body(handler, body: dict[str, object]) -> dict[str, object]:
    """Drop raw image-bearing input after the upstream request is built."""
    module_name = getattr(handler, "__module__", "")
    if module_name == openai_v1_chat_complete.__name__:
        keys = ("model", "stream", "_image_model", "_image_prompt", "_image_input_tokens")
    elif module_name == openai_v1_response.__name__:
        keys = (
            "model",
            "stream",
            "_image_model",
            "_image_prompt",
            "_image_input_tokens",
            "_image_size",
            "_image_quality",
        )
    else:
        return body
    compact = {key: body[key] for key in keys if key in body}
    body.clear()
    body.update(compact)
    return body


def create_router() -> APIRouter:
    router = APIRouter()

    @router.get("/v1/models")
    async def list_models(authorization: str | None = Header(default=None)):
        require_identity(authorization)
        try:
            return await run_in_threadpool(openai_v1_models.list_models)
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": str(exc)}) from exc

    @router.post("/v1/images/generations")
    async def generate_images(
            body: ImageGenerationRequest,
            request: Request,
            authorization: str | None = Header(default=None),
    ):
        identity = require_identity(authorization)
        payload = body.model_dump(mode="python")
        payload["base_url"] = resolve_image_base_url(request)
        call = LoggedCall(identity, "/v1/images/generations", body.model, "文生图", request_text=body.prompt)
        await filter_or_log(call, body.prompt)
        return await run_with_quota(
            call,
            openai_v1_image_generations.handle,
            payload,
            identity=identity,
            quota_type="image",
            requested_units=body.n,
            endpoint="/v1/images/generations",
            scheduler_runner=_image_scheduler_runner(body.model, payload, Lane.IMAGE_GENERATION),
            streaming=bool(payload.get("stream")),
        )

    @router.post("/v1/images/edits")
    async def edit_images(
            request: Request,
            authorization: str | None = Header(default=None),
    ):
        identity = require_identity(authorization)
        payload, image_sources, mask_sources, markup_sources = await parse_image_edit_request(request)
        budget = payload.pop("_image_input_budget", None)
        if not isinstance(budget, ImageInputBudget):
            budget = ImageInputBudget(config.image_input_max_bytes)
        prompt = str(payload["prompt"])
        model = str(payload["model"])
        comments = payload.get("comments") or []
        filter_text = prompt + "\n" + "\n".join(str(c.get("text") or "") for c in comments if isinstance(c, dict))
        call = LoggedCall(identity, "/v1/images/edits", model, "图生图", request_text=filter_text)
        await filter_or_log(call, filter_text)
        payload["images"] = await read_image_sources(image_sources, budget=budget)
        if mask_sources:
            payload["mask"] = await read_image_sources(mask_sources, budget=budget)
        if markup_sources:
            payload["markup"] = await read_image_sources(markup_sources, budget=budget)
        payload["base_url"] = resolve_image_base_url(request)
        return await run_with_quota(
            call,
            openai_v1_image_edit.handle,
            payload,
            identity=identity,
            quota_type="image",
            requested_units=int(payload.get("n") or 1),
            endpoint="/v1/images/edits",
            scheduler_runner=_image_scheduler_runner(model, payload, Lane.IMAGE_EDIT),
            streaming=bool(payload.get("stream")),
        )

    @router.post("/v1/chat/completions")
    async def create_chat_completion(body: ChatCompletionRequest, authorization: str | None = Header(default=None)):
        identity = require_identity(authorization)
        payload = body.model_dump(mode="python")
        model = str(payload.get("model") or "auto")
        request_preview = request_text(payload.get("prompt"), payload.get("messages"))
        call = LoggedCall(
            identity,
            "/v1/chat/completions",
            model,
            "文本生成",
            request_text=request_preview,
            request_shape=request_shape(payload.get("messages")),
        )
        await filter_or_log(call, request_preview)
        if is_image_chat_request(payload):
            image_lane = Lane.IMAGE_EDIT if extract_chat_image(payload) else Lane.IMAGE_GENERATION
            scheduler_runner = _image_scheduler_runner(model, payload, image_lane)
            quota_type = "image"
            requested_units = _requested_image_count(payload)
        else:
            scheduler_runner = _text_scheduler_runner(model)
            quota_type = "text"
            requested_units = 1
        return await run_with_quota(
            call,
            openai_v1_chat_complete.handle,
            payload,
            identity=identity,
            quota_type=quota_type,
            requested_units=requested_units,
            endpoint="/v1/chat/completions",
            scheduler_runner=scheduler_runner,
            streaming=bool(payload.get("stream")),
        )

    @router.post("/v1/responses")
    async def create_response(body: ResponseCreateRequest, authorization: str | None = Header(default=None)):
        identity = require_identity(authorization)
        payload = body.model_dump(mode="python")
        model = str(payload.get("model") or "auto")
        request_preview = request_text(payload.get("input"), payload.get("instructions"))
        call = LoggedCall(
            identity,
            "/v1/responses",
            model,
            "Responses",
            request_text=request_preview,
            request_shape=request_shape(payload.get("input")),
        )
        await filter_or_log(call, request_preview)
        is_image_request = openai_v1_response.has_response_image_generation_tool(payload)
        scheduler_runner = (
            _image_scheduler_runner(
                model,
                payload,
                Lane.IMAGE_EDIT if openai_v1_response.response_input_has_image(payload.get("input")) else Lane.IMAGE_GENERATION,
            )
            if is_image_request
            else _text_scheduler_runner(model)
        )
        return await run_with_quota(
            call,
            openai_v1_response.handle,
            payload,
            identity=identity,
            quota_type="image" if is_image_request else "text",
            requested_units=_requested_image_count(payload) if is_image_request else 1,
            endpoint="/v1/responses",
            scheduler_runner=scheduler_runner,
            streaming=bool(payload.get("stream")),
        )

    @router.post("/v1/messages")
    async def create_message(
            body: AnthropicMessageRequest,
            authorization: str | None = Header(default=None),
            x_api_key: str | None = Header(default=None, alias="x-api-key"),
            anthropic_version: str | None = Header(default=None, alias="anthropic-version"),
    ):
        identity = require_identity(authorization or (f"Bearer {x_api_key}" if x_api_key else None))
        payload = body.model_dump(mode="python")
        model = str(payload.get("model") or "auto")
        request_preview = request_text(payload.get("system"), payload.get("messages"), payload.get("tools"))
        call = LoggedCall(identity, "/v1/messages", model, "Messages", request_text=request_preview)
        await filter_or_log(call, request_preview)
        return await run_with_quota(
            call,
            anthropic_v1_messages.handle,
            payload,
            identity=identity,
            quota_type="text",
            requested_units=1,
            endpoint="/v1/messages",
            sse="anthropic",
            scheduler_runner=_text_scheduler_runner(model),
            streaming=bool(payload.get("stream")),
        )

    @router.post("/v1/search")
    async def search(body: SearchRequest, authorization: str | None = Header(default=None)):
        identity = require_identity(authorization)
        call = LoggedCall(identity, "/v1/search", openai_search.MODEL, "搜索", request_text=body.prompt)
        await filter_or_log(call, body.prompt)
        return await run_with_quota(
            call,
            openai_search.handle,
            body.model_dump(mode="python"),
            identity=identity,
            quota_type="text",
            requested_units=1,
            endpoint="/v1/search",
            scheduler_runner=_text_scheduler_runner(openai_search.MODEL),
        )

    @router.get("/v1/editable-file-tasks")
    async def list_editable_file_tasks(ids: str = "", authorization: str | None = Header(default=None)):
        identity = require_identity(authorization)
        task_ids = [item.strip() for item in ids.split(",") if item.strip()]
        return await run_in_threadpool(editable_file_task_service.list_tasks, identity, task_ids)

    @router.get("/files/{file_path:path}")
    async def download_editable_file(file_path: str):
        try:
            path = await run_in_threadpool(editable_file_task_service.public_file_path, file_path)
        except Exception as exc:
            raise HTTPException(status_code=404, detail={"error": "file not found"}) from exc
        return FileResponse(path, filename=path.name)

    @router.post("/v1/ppt/generations")
    async def create_ppt_task(body: EditableFileTaskRequest, request: Request, authorization: str | None = Header(default=None)):
        identity = require_identity(authorization)
        await filter_or_log(LoggedCall(identity, "/v1/ppt/generations", "gpt-5-6", "PPT生成任务", request_text=body.prompt), body.prompt)
        return await run_in_threadpool(
            editable_file_task_service.submit_ppt,
            identity,
            client_task_id=body.client_task_id or "",
            prompt=body.prompt,
            base64_images=body.base64_images,
            base_url=resolve_image_base_url(request),
        )

    @router.post("/v1/psd/generations")
    async def create_psd_task(body: EditableFileTaskRequest, request: Request, authorization: str | None = Header(default=None)):
        identity = require_identity(authorization)
        await filter_or_log(LoggedCall(identity, "/v1/psd/generations", "gpt-5-6", "PSD生成任务", request_text=body.prompt), body.prompt)
        return await run_in_threadpool(
            editable_file_task_service.submit_psd,
            identity,
            client_task_id=body.client_task_id or "",
            prompt=body.prompt,
            base64_images=body.base64_images,
            base_url=resolve_image_base_url(request),
        )

    return router
