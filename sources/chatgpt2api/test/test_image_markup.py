from __future__ import annotations

import base64
import json
from io import BytesIO

import pytest
from PIL import Image

from services.image_markup import (
    DEFAULT_MARKUP_COLOR,
    MarkupStroke,
    composite_markup_images,
    composite_markup_overlay,
    markup_strokes_to_dicts,
    normalize_markup_strokes,
    render_markup_overlay,
)


def png_bytes(color, size=(60, 40)):
    image = Image.new("RGBA", size, color)
    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def test_normalize_accepts_json_string_and_list():
    raw = json.dumps([{"points": [[0.1, 0.2], ["30%", 0.4]], "color": "#00FF00", "width": 0.01}])
    strokes = normalize_markup_strokes(raw)
    assert len(strokes) == 1
    assert strokes[0].points == [(0.1, 0.2), (0.3, 0.4)]
    assert strokes[0].color == (0, 255, 0)
    assert strokes[0].width == 0.01

    strokes = normalize_markup_strokes([{"points": [{"x": 10, "y": 20}]}])
    assert strokes[0].points == [(0.1, 0.2)]
    assert strokes[0].color == (255, 59, 48)
    assert strokes[0].width == 0.008


def test_normalize_rejects_invalid_input():
    with pytest.raises(ValueError):
        normalize_markup_strokes("not-json")
    with pytest.raises(ValueError):
        normalize_markup_strokes({"points": [[0.1, 0.2]]})
    with pytest.raises(ValueError):
        normalize_markup_strokes([{"points": []}])
    with pytest.raises(ValueError):
        normalize_markup_strokes([{"points": [[0.5]]}])
    # 150 会先按百分比解释为 1.5，仍越界
    with pytest.raises(ValueError):
        normalize_markup_strokes([{"points": [[150, 0.2]]}])
    with pytest.raises(ValueError):
        normalize_markup_strokes([{"points": [[0.1, "120%"]]}])
    with pytest.raises(ValueError):
        normalize_markup_strokes([{"points": [[0.1, 0.2]], "color": "#GGHHII"}])
    with pytest.raises(ValueError):
        normalize_markup_strokes([{"points": [[0.1, 0.2]], "width": 50}])
    with pytest.raises(ValueError):
        normalize_markup_strokes([{"points": [[0.1, 0.2]]}] * 201)


def test_markup_strokes_to_dicts_roundtrip():
    stroke = MarkupStroke(points=[(0.123456, 0.5)], color=(1, 2, 3), width=0.0125)
    dicts = markup_strokes_to_dicts([stroke])
    assert dicts[0]["color"] == "#010203"
    restored = normalize_markup_strokes(dicts)
    assert restored[0].points == stroke.points
    assert restored[0].color == stroke.color
    assert restored[0].width == stroke.width


def test_render_overlay_draws_opaque_stroke_on_transparent_canvas():
    overlay = render_markup_overlay(
        [MarkupStroke(points=[(0.1, 0.5), (0.9, 0.5)], color=(255, 0, 0), width=0.1)],
        100,
        50,
    )
    with Image.open(BytesIO(overlay)) as image:
        assert image.size == (100, 50)
        assert image.mode == "RGBA"
        # 线条中点应为不透明红色
        midpoint = image.getpixel((50, 25))
        assert midpoint[3] > 200
        assert midpoint[0] > 180
        # 远离笔迹的区域保持透明
        assert image.getpixel((50, 3))[3] == 0


def test_composite_overlay_blends_marks_into_image():
    source = png_bytes((0, 0, 255, 255))
    overlay = render_markup_overlay(
        [MarkupStroke(points=[(0.5, 0.5)], color=(255, 0, 0), width=0.2)],
        60,
        40,
    )
    composited = composite_markup_overlay(source, overlay)
    with Image.open(BytesIO(composited)) as image:
        assert image.size == (60, 40)
        center = image.getpixel((30, 20))
        assert center[0] > 200 and center[2] < 200
        corner = image.getpixel((2, 2))
        assert corner[:3] == (0, 0, 255)


def test_composite_markup_images_renders_strokes_and_renames_png():
    images = [(png_bytes((0, 255, 0, 255)), "source.jpg", "image/jpeg")]
    strokes = normalize_markup_strokes([{"points": [[0.5, 0.5]], "width": 0.2}])
    result = composite_markup_images(images, None, strokes)
    assert len(result) == 1
    data, filename, mime_type = result[0]
    assert filename == "source.png"
    assert mime_type == "image/png"
    with Image.open(BytesIO(data)) as image:
        assert image.format == "PNG"
        center = image.getpixel((30, 20))
        assert center[0] > 200 and center[1] < 200


def test_composite_markup_images_without_markup_passthrough():
    images = [(png_bytes((0, 255, 0, 255)), "source.png", "image/png")]
    assert composite_markup_images(images, None, None) == images


def test_default_color_matches_web_default():
    assert DEFAULT_MARKUP_COLOR == "#FF3B30"
    assert normalize_markup_strokes([{"points": [[0, 0]]}])[0].color == (255, 59, 48)


# ---- 贯通层：payload 构建 / 编辑协议 / HTTP 解析 / 任务服务 ----

from api.image_inputs import _payload_from_fields  # noqa: E402
from fastapi import HTTPException  # noqa: E402
from services.image_task_service import image_task_service  # noqa: E402
from services.openai_backend_api import build_image_generation_payload  # noqa: E402
from services.protocol import openai_v1_image_edit as edit_module  # noqa: E402
from services.protocol.conversation import ImageGenerationError  # noqa: E402


def _reference(file_id: str) -> dict:
    return {
        "file_id": file_id,
        "file_name": "image-markup-preview.png",
        "file_size": 2790820,
        "mime_type": "image/png",
        "width": 1024,
        "height": 1536,
    }


def test_build_image_generation_payload_with_markup_carries_flow_tag():
    payload = build_image_generation_payload(
        "把圆圈内的霓虹招牌换成绿色",
        [_reference("file_composite")],
        "gpt-5-6",
        "",
        markup=True,
    )
    message = payload["messages"][0]
    assert message["metadata"]["image_gen_flow_tags"] == ["markup"]
    # 与评论编辑同样的实测结论：全新会话 + 上传图必须省略 dalle.from_client.operation
    assert "dalle" not in message["metadata"]
    assert message["content"]["parts"][0]["asset_pointer"] == "file-service://file_composite"
    assert message["content"]["parts"][1] == "@创建图像 把圆圈内的霓虹招牌换成绿色"


def test_build_image_generation_payload_without_markup_has_no_flow_tag():
    payload = build_image_generation_payload("edit", [_reference("file_a")], "gpt-5-6", "")
    assert "image_gen_flow_tags" not in payload["messages"][0]["metadata"]


def test_edit_build_request_composites_strokes_and_sets_markup_flag():
    body = {
        "prompt": "把圆圈内的招牌换成绿色",
        "model": "gpt-image-2",
        "images": [(png_bytes((0, 0, 255, 255)), "src.png", "image/png")],
        "markup_strokes": [{"points": [[0.5, 0.5]], "width": 0.2}],
    }
    request = edit_module.build_request(body)
    assert request.markup is True
    assert body["markup"] == []
    assert body["images"] == []
    assert len(request.images) == 1
    with Image.open(BytesIO(base64.b64decode(request.images[0]))) as image:
        center = image.getpixel((30, 20))
        assert center[0] > 200 and center[2] < 200


def test_edit_build_request_composites_overlay_file():
    overlay = render_markup_overlay(
        [MarkupStroke(points=[(0.5, 0.5)], color=(255, 0, 0), width=0.2)], 60, 40
    )
    body = {
        "prompt": "edit",
        "model": "gpt-image-2",
        "images": [(png_bytes((0, 0, 255, 255)), "src.png", "image/png")],
        "markup": [(overlay, "overlay.png", "image/png")],
    }
    request = edit_module.build_request(body)
    assert request.markup is True
    with Image.open(BytesIO(base64.b64decode(request.images[0]))) as image:
        center = image.getpixel((30, 20))
        assert center[0] > 200 and center[2] < 200


def test_edit_build_request_rejects_invalid_markup_strokes():
    body = {
        "prompt": "edit",
        "model": "gpt-image-2",
        "images": [(png_bytes((0, 0, 255, 255)), "src.png", "image/png")],
        "markup_strokes": [{"points": [[150, 0.2]]}],
    }
    with pytest.raises(ImageGenerationError) as excinfo:
        edit_module.build_request(body)
    assert excinfo.value.status_code == 400
    assert excinfo.value.code == "invalid_image_markup"


def test_edit_build_request_without_markup_keeps_flag_false():
    body = {
        "prompt": "edit",
        "model": "gpt-image-2",
        "images": [(png_bytes((0, 0, 255, 255)), "src.png", "image/png")],
    }
    request = edit_module.build_request(body)
    assert request.markup is False


def test_payload_from_fields_accepts_markup_strokes_only():
    payload = _payload_from_fields(
        {"prompt": "", "markup_strokes": json.dumps([{"points": [[0.25, "50%"]]}])}
    )
    assert payload["prompt"] == ""
    assert payload["markup_strokes"] == [{"points": [[0.25, 0.5]], "color": "#FF3B30", "width": 0.008}]

    with pytest.raises(HTTPException) as excinfo:
        _payload_from_fields({"prompt": "", "markup_strokes": "bad json"})
    assert excinfo.value.status_code == 400

    with pytest.raises(HTTPException) as excinfo:
        _payload_from_fields({"prompt": "", "markup_strokes": []})
    assert excinfo.value.status_code == 400


def test_task_edit_payload_carries_markup():
    overlay = render_markup_overlay(
        [MarkupStroke(points=[(0.5, 0.5)], color=(255, 0, 0), width=0.1)], 60, 40
    )
    payload = image_task_service._build_edit_payload(
        prompt="p",
        model="gpt-image-2",
        size=None,
        quality="auto",
        base_url="http://test",
        images=[(png_bytes((0, 0, 255, 255)), "src.png", "image/png")],
        masks=None,
        markup_overlays=[(overlay, "overlay.png", "image/png")],
        markup_strokes=[{"points": [[0.5, 0.5]], "color": "#FF3B30", "width": 0.008}],
    )
    assert len(payload["markup"]) == 1
    assert payload["markup_strokes"] == [{"points": [[0.5, 0.5]], "color": "#FF3B30", "width": 0.008}]
    # spool 描述符应带内部路径键，物化后回到字节元组
    materialized = image_task_service._materialize_payload(payload)
    assert len(materialized["markup"]) == 1
    data, filename, mime_type = materialized["markup"][0]
    assert filename == "overlay.png"
    assert mime_type == "image/png"
    assert data.startswith(b"\x89PNG")
    image_task_service._cleanup_payload_spool(payload)


def test_task_edit_payload_without_markup_has_empty_lists():
    payload = image_task_service._build_edit_payload(
        prompt="p",
        model="gpt-image-2",
        size=None,
        quality="auto",
        base_url="http://test",
        images=None,
        masks=None,
    )
    assert payload["markup"] == []
    assert "markup_strokes" not in payload
    assert "image_gen_flow_tags" not in payload
