from __future__ import annotations

from typing import Any, Iterable, Iterator

from services.protocol.conversation import (
    ConversationRequest,
    collect_image_outputs,
    count_text_tokens,
    stream_image_chunks,
    stream_image_outputs_with_pool,
)
from utils.image_tokens import count_image_output_items_tokens, image_usage


def build_request(body: dict[str, Any]) -> ConversationRequest:
    prompt = str(body.get("prompt") or "")
    model = str(body.get("model") or "gpt-image-2")
    n = int(body.get("n") or 1)
    size = body.get("size")
    quality = str(body.get("quality") or "auto")
    response_format = str(body.get("response_format") or "b64_json")
    progress_callback = body.get("progress_callback")
    return ConversationRequest(
        prompt=prompt,
        model=model,
        n=n,
        size=size,
        quality=quality,
        response_format=response_format,
        message_as_error=True,
        progress_callback=progress_callback,
    )


def format_outputs(
    outputs: Iterable[Any],
    body: dict[str, Any],
) -> dict[str, Any] | Iterator[dict[str, Any]]:
    prompt = str(body.get("prompt") or "")
    model = str(body.get("model") or "gpt-image-2")
    size = body.get("size")
    quality = str(body.get("quality") or "auto")
    if body.get("stream"):
        return stream_image_chunks(outputs)
    result = collect_image_outputs(outputs)
    result["usage"] = image_usage(
        input_text_tokens=count_text_tokens(prompt, model),
        output_tokens=count_image_output_items_tokens(result.get("data"), size, quality),
    )
    return result


def handle(body: dict[str, Any]) -> dict[str, Any] | Iterator[dict[str, Any]]:
    from services.protocol import image_upstream
    if image_upstream.configured():
        return format_outputs(image_upstream.generate(body), body)
    return format_outputs(stream_image_outputs_with_pool(build_request(body)), body)
