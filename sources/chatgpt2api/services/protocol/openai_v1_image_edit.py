from __future__ import annotations

from io import BytesIO
from typing import Any, Iterable, Iterator

from PIL import Image

from services.image_comments import normalize_image_comments
from services.image_markup import composite_markup_images, normalize_markup_strokes
from services.protocol.conversation import (
    ConversationRequest,
    ImageGenerationError,
    collect_image_outputs,
    count_text_tokens,
    encode_images,
    stream_image_chunks,
    stream_image_outputs_with_pool,
)
from utils.image_tokens import count_image_inputs_tokens, count_image_output_items_tokens, image_usage


def _composite_mask(
    images: list[tuple[bytes, str, str]],
    masks: list[tuple[bytes, str, str]],
) -> list[tuple[bytes, str, str]]:
    """将 mask 的 alpha 通道合成到图片中，标识需要编辑的区域。
    
    mask 的透明区域（低 alpha）= 需要编辑的区域，
    mask 的不透明区域（高 alpha）= 保留的区域。
    如果无 mask 则返回原图。
    """
    if not masks:
        return images
    result: list[tuple[bytes, str, str]] = []
    for i, (data, filename, mime_type) in enumerate(images):
        mask_data = masks[i][0] if i < len(masks) else masks[-1][0]
        with (
            BytesIO(data) as source_buffer,
            BytesIO(mask_data) as mask_buffer,
            Image.open(source_buffer) as source_image,
            Image.open(mask_buffer) as mask_img,
        ):
            img = source_image.convert("RGBA")
            try:
                if mask_img.mode == "RGBA":
                    alpha = mask_img.getchannel("A")
                elif mask_img.mode == "L":
                    alpha = mask_img.copy()
                else:
                    alpha = mask_img.convert("L")
                resized_alpha = alpha.resize(img.size, Image.LANCZOS)
                alpha.close()
                img.putalpha(resized_alpha)
                resized_alpha.close()
                buf = BytesIO()
                try:
                    img.save(buf, format="PNG")
                    result.append((buf.getvalue(), filename, "image/png"))
                finally:
                    buf.close()
            finally:
                img.close()
    return result


def build_request(body: dict[str, Any]) -> ConversationRequest:
    prompt = str(body.get("prompt") or "")
    images = body.get("images") or []
    masks = body.get("mask") or []
    images = _composite_mask(images, masks)
    model = str(body.get("model") or "gpt-image-2")
    n = int(body.get("n") or 1)
    size = body.get("size")
    quality = str(body.get("quality") or "auto")
    response_format = str(body.get("response_format") or "b64_json")
    progress_callback = body.get("progress_callback")
    try:
        comments = normalize_image_comments(body.get("comments"))
    except ValueError as exc:
        raise ImageGenerationError(
            str(exc),
            status_code=400,
            error_type="invalid_request_error",
            code="invalid_image_comments",
        ) from exc
    try:
        markup_strokes = normalize_markup_strokes(body.get("markup_strokes"))
    except ValueError as exc:
        raise ImageGenerationError(
            str(exc),
            status_code=400,
            error_type="invalid_request_error",
            code="invalid_image_markup",
        ) from exc
    markup_overlays = body.get("markup") or []
    markup = bool(markup_overlays or markup_strokes)
    if markup:
        # 网页端「标注」编辑：把笔迹合成进输入图像素后再走普通编辑链路
        images = composite_markup_images(images, markup_overlays, markup_strokes)
    encoded_images = encode_images(images)
    input_image_tokens = count_image_inputs_tokens(images, model)
    body["_input_image_tokens"] = input_image_tokens
    body["images"] = []
    body["mask"] = []
    body["markup"] = []
    if not encoded_images:
        raise ImageGenerationError("image is required")
    return ConversationRequest(
        prompt=prompt,
        model=model,
        n=n,
        size=size,
        quality=quality,
        response_format=response_format,
        images=encoded_images,
        message_as_error=True,
        progress_callback=progress_callback,
        comments=comments,
        markup=markup,
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
    input_image_tokens = body.get("_input_image_tokens")
    if input_image_tokens is None:
        input_image_tokens = count_image_inputs_tokens(body.get("images") or [], model)
    result["usage"] = image_usage(
        input_text_tokens=count_text_tokens(prompt, model),
        input_image_tokens=int(input_image_tokens or 0),
        output_tokens=count_image_output_items_tokens(result.get("data"), size, quality),
    )
    return result


def handle(body: dict[str, Any]) -> dict[str, Any] | Iterator[dict[str, Any]]:
    from services.protocol import image_upstream
    request = build_request(body)
    if image_upstream.configured():
        return format_outputs(image_upstream.edit(body, request), body)
    return format_outputs(stream_image_outputs_with_pool(request), body)
