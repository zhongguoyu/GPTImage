from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from api.image_inputs import ImageInputBudget, parse_image_edit_request, read_image_sources
from api.support import _quota_exhausted_response, require_identity, require_admin, resolve_image_base_url
from services.content_filter import check_request
from services.config import config
from services.image_task_service import image_task_service
from services.log_service import LoggedCall
from services.quota_service import QuotaExceededError, QuotaSettlementError
from services.scheduler.errors import SchedulerError
from services.user_key_concurrency import UserKeyConcurrencyLimitError


class ImageGenerationTaskRequest(BaseModel):
    client_task_id: str = Field(..., min_length=1)
    prompt: str = Field(..., min_length=1)
    model: str = "gpt-image-2"
    size: str | None = None
    quality: str = "auto"


class ResumePollRequest(BaseModel):
    extra_timeout_secs: float = Field(default=30.0, ge=5.0, le=120.0)


def _parse_task_ids(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


async def filter_or_log(call: LoggedCall, text: str) -> None:
    try:
        await run_in_threadpool(check_request, text)
    except HTTPException as exc:
        call.log("调用失败", status="failed", error=str(exc.detail))
        raise


def create_router() -> APIRouter:
    router = APIRouter()

    @router.get("/api/image-status")
    async def image_status(model: str = Query(default="gpt-image-2"), authorization: str | None = Header(default=None)):
        require_admin(authorization)
        from services.image_operational_status import status
        return await run_in_threadpool(status, model)

    @router.get("/api/image-tasks")
    async def list_image_tasks(
        ids: str = Query(default=""),
        authorization: str | None = Header(default=None),
    ):
        identity = require_identity(authorization)
        return await run_in_threadpool(image_task_service.list_tasks, identity, _parse_task_ids(ids))

    @router.post("/api/image-tasks/generations")
    async def create_generation_task(
        body: ImageGenerationTaskRequest,
        request: Request,
        authorization: str | None = Header(default=None),
    ):
        identity = require_identity(authorization)
        await filter_or_log(LoggedCall(identity, "/api/image-tasks/generations", body.model, "文生图任务", request_text=body.prompt), body.prompt)
        try:
            return await image_task_service.submit_generation_async(
                identity,
                client_task_id=body.client_task_id,
                prompt=body.prompt,
                model=body.model,
                size=body.size,
                quality=body.quality,
                base_url=resolve_image_base_url(request),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
        except QuotaExceededError as exc:
            raise HTTPException(status_code=429, detail=_quota_exhausted_response(exc)) from exc
        except UserKeyConcurrencyLimitError as exc:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": {
                        "message": str(exc),
                        "type": "rate_limit_error",
                        "code": "user_key_concurrency_limit",
                    }
                },
                headers={"Retry-After": str(exc.retry_after_seconds)},
            ) from exc
        except SchedulerError as exc:
            headers = {"Retry-After": str(max(1, int(exc.retry_after_seconds)))} if exc.retry_after_seconds else None
            raise HTTPException(status_code=exc.status_code, detail={"error": str(exc)}, headers=headers) from exc

    @router.post("/api/image-tasks/edits")
    async def create_edit_task(
        request: Request,
        authorization: str | None = Header(default=None),
    ):
        identity = require_identity(authorization)
        payload, image_sources, mask_sources, markup_sources = await parse_image_edit_request(request)
        budget = payload.pop("_image_input_budget", None)
        if not isinstance(budget, ImageInputBudget):
            budget = ImageInputBudget(config.image_input_max_bytes)
        client_task_id = str(payload.get("client_task_id") or "").strip()
        if not client_task_id:
            raise HTTPException(status_code=400, detail={"error": "client_task_id is required"})
        prompt = str(payload["prompt"])
        model = str(payload["model"])
        comments = payload.get("comments") or []
        filter_text = prompt + "\n" + "\n".join(str(c.get("text") or "") for c in comments if isinstance(c, dict))
        await filter_or_log(LoggedCall(identity, "/api/image-tasks/edits", model, "图生图任务", request_text=filter_text), filter_text)
        images = await read_image_sources(image_sources, budget=budget)
        masks = await read_image_sources(mask_sources, budget=budget) if mask_sources else None
        markup_overlays = await read_image_sources(markup_sources, budget=budget) if markup_sources else None
        try:
            return await image_task_service.submit_edit_async(
                identity,
                client_task_id=client_task_id,
                prompt=prompt,
                model=model,
                size=payload["size"],
                quality=payload["quality"],
                base_url=resolve_image_base_url(request),
                images=images,
                masks=masks,
                comments=comments,
                markup_overlays=markup_overlays,
                markup_strokes=payload.get("markup_strokes") or [],
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
        except QuotaExceededError as exc:
            raise HTTPException(status_code=429, detail=_quota_exhausted_response(exc)) from exc
        except UserKeyConcurrencyLimitError as exc:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": {
                        "message": str(exc),
                        "type": "rate_limit_error",
                        "code": "user_key_concurrency_limit",
                    }
                },
                headers={"Retry-After": str(exc.retry_after_seconds)},
            ) from exc
        except SchedulerError as exc:
            headers = {"Retry-After": str(max(1, int(exc.retry_after_seconds)))} if exc.retry_after_seconds else None
            raise HTTPException(status_code=exc.status_code, detail={"error": str(exc)}, headers=headers) from exc
        finally:
            del images
            if masks is not None:
                del masks
            if markup_overlays is not None:
                del markup_overlays

    @router.post("/api/image-tasks/{task_id}/resume-poll")
    async def resume_image_poll(
        task_id: str,
        body: ResumePollRequest,
        request: Request,
        authorization: str | None = Header(default=None),
    ):
        identity = require_identity(authorization)
        try:
            return await image_task_service.resume_poll_async(
                identity,
                task_id,
                body.extra_timeout_secs,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
        except QuotaSettlementError as exc:
            raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc
        except UserKeyConcurrencyLimitError as exc:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": {
                        "message": str(exc),
                        "type": "rate_limit_error",
                        "code": "user_key_concurrency_limit",
                    }
                },
                headers={"Retry-After": str(exc.retry_after_seconds)},
            ) from exc
        except SchedulerError as exc:
            headers = {"Retry-After": str(max(1, int(exc.retry_after_seconds)))} if exc.retry_after_seconds else None
            raise HTTPException(status_code=exc.status_code, detail={"error": str(exc)}, headers=headers) from exc

    return router
