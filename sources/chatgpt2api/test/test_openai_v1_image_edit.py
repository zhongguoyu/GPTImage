from __future__ import annotations

from io import BytesIO

from PIL import Image

from services.protocol import openai_v1_image_edit as edit_module
from services.protocol.conversation import ImageOutput


def png_bytes(color):
    image = Image.new("RGBA", (4, 4), color)
    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def test_image_edit_composes_mask_once_between_build_and_format(monkeypatch):
    calls = 0
    original = edit_module._composite_mask

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(edit_module, "_composite_mask", counted)
    body = {
        "prompt": "edit",
        "model": "gpt-image-2",
        "images": [(png_bytes((255, 0, 0, 255)), "source.png", "image/png")],
        "mask": [(png_bytes((0, 0, 0, 80)), "mask.png", "image/png")],
        "response_format": "b64_json",
    }

    edit_module.build_request(body)
    result = edit_module.format_outputs(
        [ImageOutput(kind="result", model="gpt-image-2", index=1, total=1, data=[{"b64_json": "ZmFrZQ=="}])],
        body,
    )

    assert calls == 1
    assert result["usage"]["input_tokens"] >= 0
    assert body["images"] == []
    assert body["mask"] == []


def test_image_edit_format_outputs_preserves_direct_call_image_usage():
    body = {
        "prompt": "edit",
        "model": "gpt-image-2",
        "images": [(png_bytes((255, 0, 0, 255)), "source.png", "image/png")],
        "response_format": "b64_json",
    }

    result = edit_module.format_outputs(
        [ImageOutput(kind="result", model="gpt-image-2", index=1, total=1, data=[{"b64_json": "ZmFrZQ=="}])],
        body,
    )

    usage = result["usage"]
    assert usage["input_tokens"] >= usage["input_tokens_details"]["image_tokens"]
    assert usage["input_tokens_details"]["image_tokens"] > 0
