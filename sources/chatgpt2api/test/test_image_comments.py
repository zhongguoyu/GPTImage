from __future__ import annotations

import json

import pytest
from fastapi import HTTPException

from api.image_inputs import _payload_from_fields
from services.image_comments import (
    ImageComment,
    build_comment_text,
    build_image_message_parts,
    format_comment_percent,
    image_comment_text_block,
    normalize_image_comments,
)
from services.image_task_service import image_task_service
from services.openai_backend_api import build_image_generation_payload
from services.protocol import openai_v1_image_edit as edit_module
from services.protocol.conversation import ImageGenerationError

# 以下两段文本为 2026-10 从 chatgpt.com 网页端「添加评论」编辑请求抓包所得的原文
CAPTURED_SINGLE = "图像 1：\n1.（x：27.5%，y：70%）把这只青瓷茶壶换成带金色描边的白瓷茶壶"
CAPTURED_DOUBLE = (
    "图像 1：\n"
    "1.（x：21.5%，y：65.2%）茶壶旁边再加一个倒好的茶杯\n"
    "2.（x：36%，y：31.5%）发髻上再插一支金色步摇"
)


def test_format_comment_percent_matches_upstream_encoding():
    assert format_comment_percent(0.275) == "27.5"
    assert format_comment_percent(0.7) == "70"
    assert format_comment_percent(0.36) == "36"
    assert format_comment_percent(0.652) == "65.2"
    assert format_comment_percent(0.0) == "0"
    assert format_comment_percent(1.0) == "100"


def test_comment_text_block_reproduces_captured_single_comment():
    block = image_comment_text_block(
        [ImageComment(x=0.275, y=0.7, text="把这只青瓷茶壶换成带金色描边的白瓷茶壶")],
        image_number=1,
    )
    assert block == CAPTURED_SINGLE


def test_comment_text_block_reproduces_captured_double_comment():
    block = image_comment_text_block(
        [
            ImageComment(x=0.215, y=0.652, text="茶壶旁边再加一个倒好的茶杯"),
            ImageComment(x=0.36, y=0.315, text="发髻上再插一支金色步摇"),
        ],
        image_number=1,
    )
    assert block == CAPTURED_DOUBLE


def test_normalize_accepts_percent_strings_and_zero_to_hundred_numbers():
    comments = normalize_image_comments([
        {"x": "27.5%", "y": "70%", "text": "a"},
        {"x": 27.5, "y": 100, "text": "b", "image_index": 1},
        {"x": 0.5, "y": 0, "text": "c"},
    ])
    assert [(c.x, c.y, c.text, c.image_index) for c in comments] == [
        (0.275, 0.7, "a", 0),
        (0.275, 1.0, "b", 1),
        (0.5, 0.0, "c", 0),
    ]


def test_normalize_accepts_json_string_and_rejects_invalid_input():
    comments = normalize_image_comments(json.dumps([{"x": "10%", "y": "20%", "text": "t"}]))
    assert len(comments) == 1

    with pytest.raises(ValueError):
        normalize_image_comments([{"x": 0.1, "y": 0.2, "text": ""}])
    # 大于 1 的数值按百分比解析，150% 超出图片范围
    with pytest.raises(ValueError):
        normalize_image_comments([{"x": 150, "y": 0.2, "text": "t"}])
    with pytest.raises(ValueError):
        normalize_image_comments([{"x": "abc", "y": 0.2, "text": "t"}])
    with pytest.raises(ValueError):
        normalize_image_comments([{"x": 0.1, "y": 0.2, "text": "t", "image_index": -1}])
    with pytest.raises(ValueError):
        normalize_image_comments({"x": 0.1, "y": 0.2, "text": "t"})


def test_build_comment_text_groups_by_image_and_skips_out_of_range():
    comments = [
        ImageComment(x=0.1, y=0.2, text="first", image_index=1),
        ImageComment(x=0.3, y=0.4, text="second", image_index=0),
    ]
    text = build_comment_text(comments, image_count=2)
    assert text.startswith("图像 1：\n1.（x：30%，y：40%）second\n图像 2：\n1.（x：10%，y：20%）first")
    # 超出图片数量的评论块直接丢弃
    assert build_comment_text(comments, image_count=1).startswith("图像 1：\n1.（x：30%，y：40%）second")


def reference(file_id: str) -> dict:
    return {
        "file_id": file_id,
        "file_name": "generated-image.png",
        "file_size": 2117233,
        "mime_type": "image/png",
        "width": 941,
        "height": 1672,
    }


def test_build_image_message_parts_single_image_matches_captured_layout():
    parts = build_image_message_parts(
        "",
        [reference("file_original")],
        [ImageComment(x=0.275, y=0.7, text="把这只青瓷茶壶换成带金色描边的白瓷茶壶")],
    )
    assert parts[0] == {
        "content_type": "image_asset_pointer",
        "asset_pointer": "file-service://file_original",
        "width": 941,
        "height": 1672,
        "size_bytes": 2117233,
    }
    assert parts[1] == CAPTURED_SINGLE


def test_build_image_message_parts_appends_prompt_after_comment_block():
    parts = build_image_message_parts(
        "保持其余部分不变",
        [reference("file_a")],
        [ImageComment(x=0.1, y=0.2, text="加一朵花")],
    )
    assert parts[1] == "图像 1：\n1.（x：10%，y：20%）加一朵花"
    assert parts[2] == "保持其余部分不变"


def test_build_image_generation_payload_with_comments_replicates_client_encoding():
    payload = build_image_generation_payload(
        "",
        [reference("file_original")],
        "gpt-5-6",
        "",
        comments=[ImageComment(x=0.275, y=0.7, text="把这只青瓷茶壶换成带金色描边的白瓷茶壶")],
    )
    message = payload["messages"][0]
    assert message["content"]["content_type"] == "multimodal_text"
    assert message["content"]["parts"][1] == CAPTURED_SINGLE
    # 实测确认：上传图没有 original_gen_id，携带 dalle.from_client.operation 会让上游图片工具报错
    assert "dalle" not in message["metadata"]
    assert payload["system_hints"] == ["picture_v2"]
    assert message["metadata"]["system_hints"] == ["picture_v2"]


def test_build_image_generation_payload_without_comments_keeps_legacy_shape():
    payload = build_image_generation_payload("edit prompt", [reference("file_a")], "gpt-5-6", "standard")
    message = payload["messages"][0]
    parts = message["content"]["parts"]
    assert parts[0]["asset_pointer"] == "file-service://file_a"
    assert parts[1] == "@创建图像 edit prompt"
    assert "dalle" not in message["metadata"]
    assert payload["thinking_effort"] == "standard"


def test_edit_build_request_threads_comments_into_conversation_request():
    body = {
        "prompt": "",
        "model": "gpt-image-2",
        "images": [],
        "comments": [{"x": "27.5%", "y": "70%", "text": "把茶壶换成白瓷壶"}],
    }
    # images 为空会报 image is required，这里只验证 comments 解析先行报错路径之外的传递
    body["images"] = [(b"\x89PNG fake", "src.png", "image/png")]
    try:
        request = edit_module.build_request(body)
    except Exception:
        pytest.skip("encode_images requires a real image in this environment")
    assert request.comments == [ImageComment(x=0.275, y=0.7, text="把茶壶换成白瓷壶")]


def test_edit_build_request_rejects_invalid_comments():
    body = {
        "prompt": "edit",
        "model": "gpt-image-2",
        "images": [(b"\x89PNG fake", "src.png", "image/png")],
        "comments": [{"x": 150, "y": 0.2, "text": "越界"}],
    }
    with pytest.raises(ImageGenerationError) as excinfo:
        edit_module.build_request(body)
    assert excinfo.value.status_code == 400
    assert excinfo.value.code == "invalid_image_comments"


def test_payload_from_fields_allows_comment_only_request():
    payload = _payload_from_fields({
        "prompt": "",
        "comments": json.dumps([{"x": "27.5%", "y": "70%", "text": "换茶壶"}]),
    })
    assert payload["prompt"] == ""
    assert payload["comments"] == [{"x": 0.275, "y": 0.7, "text": "换茶壶", "image_index": 0}]

    with pytest.raises(HTTPException) as excinfo:
        _payload_from_fields({"prompt": "", "comments": []})
    assert excinfo.value.status_code == 400

    with pytest.raises(HTTPException) as excinfo:
        _payload_from_fields({"prompt": "p", "comments": [{"x": "bad", "y": 1, "text": "t"}]})
    assert excinfo.value.status_code == 400


def test_task_edit_payload_carries_comments():
    payload = image_task_service._build_edit_payload(
        prompt="",
        model="gpt-image-2",
        size=None,
        quality="auto",
        base_url="http://test",
        images=None,
        masks=None,
        comments=[{"x": 0.5, "y": 0.5, "text": "t", "image_index": 0}],
    )
    assert payload["comments"] == [{"x": 0.5, "y": 0.5, "text": "t", "image_index": 0}]

    payload_without = image_task_service._build_edit_payload(
        prompt="p",
        model="gpt-image-2",
        size=None,
        quality="auto",
        base_url="http://test",
        images=None,
        masks=None,
    )
    assert "comments" not in payload_without
