"""图片评论（锚点标注）编辑支持。

复刻 chatgpt.com 网页端「添加评论」图像编辑的请求编码（2026-10 抓包确认）：
- 输入图 asset_pointer 之后跟随该图的评论文本块；
- 文本块格式为 `图像 N：\\n序号.（x：X%，y：Y%）评论内容`，
  x/y 为图内归一化百分比（保留 1 位小数、去掉尾零），
  冒号/括号/逗号使用全角字符（U+FF1A / U+FF08 / U+FF09 / U+FF0C）。

实测注意事项：网页端编辑同会话已生成的图片时还会携带
metadata.dalle.from_client.operation = {type: "transformation", original_file_id, original_gen_id}；
代理的编辑链路是全新会话 + 刚上传的图片（没有 original_gen_id），此时携带该
metadata 会导致上游图片工具直接报错，因此必须省略。

上游响应不变：仍通过 image_gen 工具消息返回新图（file-service:// 或 sediment:// 指针）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

# 单请求评论数量与单条文本长度上限
MAX_COMMENTS_PER_REQUEST = 20
MAX_COMMENT_TEXT_CHARS = 2000


@dataclass
class ImageComment:
    """一条锚点评论：坐标为图片内归一化 [0,1] 坐标，image_index 从 0 开始。"""

    x: float
    y: float
    text: str
    image_index: int = 0


def format_comment_percent(value: float) -> str:
    """把 [0,1] 坐标格式化为上游文本使用的百分比字符串（如 27.5 / 70）。"""
    return f"{value * 100:.1f}".rstrip("0").rstrip(".")


def _parse_comment_coordinate(value: Any, field_name: str) -> float:
    """解析坐标：接受 [0,1] 数值、"27.5%" 或 0-100 的数值。"""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"comment {field_name} must be a number between 0 and 1, a percentage string, or 0-100")
    percent = False
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"comment {field_name} must not be empty")
        if text.endswith("%"):
            percent = True
            text = text[:-1].strip()
        try:
            number = float(text)
        except ValueError as exc:
            raise ValueError(f"comment {field_name} must be a number between 0 and 1, a percentage string, or 0-100") from exc
    elif isinstance(value, (int, float)):
        number = float(value)
    else:
        raise ValueError(f"comment {field_name} must be a number between 0 and 1, a percentage string, or 0-100")
    if percent or number > 1.0:
        number = number / 100.0
    if not 0.0 <= number <= 1.0:
        raise ValueError(f"comment {field_name} must be within the image (0-100%)")
    return number


def normalize_image_comments(value: Any) -> list[ImageComment]:
    """归一化评论列表：接受 JSON 字符串、dict 列表或已归一化的列表，非法输入抛 ValueError。"""
    if value is None or value == []:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("comments must be valid JSON") from exc
    if not isinstance(value, list):
        raise ValueError("comments must be a list")
    comments: list[ImageComment] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("each comment must be an object with x, y and text")
        text = str(item.get("text") or "").strip()
        if not text:
            raise ValueError("comment text is required")
        if len(text) > MAX_COMMENT_TEXT_CHARS:
            raise ValueError(f"comment text exceeds {MAX_COMMENT_TEXT_CHARS} characters")
        x = _parse_comment_coordinate(item.get("x"), "x")
        y = _parse_comment_coordinate(item.get("y"), "y")
        raw_index = item.get("image_index", 0)
        try:
            image_index = int(raw_index or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("comment image_index must be an integer") from exc
        if image_index < 0:
            raise ValueError("comment image_index must not be negative")
        comments.append(ImageComment(x=x, y=y, text=text, image_index=image_index))
    if len(comments) > MAX_COMMENTS_PER_REQUEST:
        raise ValueError(f"too many comments (max {MAX_COMMENTS_PER_REQUEST})")
    return comments


def image_comments_to_dicts(comments: list[ImageComment]) -> list[dict[str, Any]]:
    """转成可 JSON 持久化的 dict 列表（任务状态存储用）。"""
    return [
        {"x": comment.x, "y": comment.y, "text": comment.text, "image_index": comment.image_index}
        for comment in comments
    ]


def group_image_comments(comments: list[ImageComment] | None) -> dict[int, list[ImageComment]]:
    """按 image_index 分组，保持原有顺序。"""
    grouped: dict[int, list[ImageComment]] = {}
    for comment in comments or []:
        grouped.setdefault(comment.image_index, []).append(comment)
    return grouped


def image_comment_text_block(comments: list[ImageComment], image_number: int) -> str:
    """生成单张图片的评论文本块（image_number 从 1 开始，与上游编码一致）。"""
    lines = [f"图像 {image_number}："]
    for order, comment in enumerate(comments, start=1):
        lines.append(
            f"{order}.（x：{format_comment_percent(comment.x)}%，y：{format_comment_percent(comment.y)}%）{comment.text}"
        )
    return "\n".join(lines)


def build_comment_text(comments: list[ImageComment] | None, image_count: int) -> str:
    """生成全部评论的合并文本块，多图时按图片顺序拼接（图像 N：编号与图对应）。"""
    grouped = group_image_comments(comments)
    blocks = [
        image_comment_text_block(image_comments, image_index + 1)
        for image_index, image_comments in sorted(grouped.items())
        if image_index < image_count
    ]
    return "\n".join(blocks)


def build_image_message_parts(
    prompt: str,
    references: list[dict[str, Any]],
    comments: list[ImageComment] | None,
) -> list[Any]:
    """构造 multimodal_text 的 parts：所有图片指针在前，随后是评论块与全局 prompt。

    与网页端编码一致：单图时 parts = [指针, "图像 1：\\n..."]；
    多图时评论块按图像编号合并为一个文本部分（与本仓库多图输入编码风格一致）。
    """
    parts: list[Any] = [
        {
            "content_type": "image_asset_pointer",
            "asset_pointer": f"file-service://{item['file_id']}",
            "width": item["width"],
            "height": item["height"],
            "size_bytes": item["file_size"],
        }
        for item in references
    ]
    comment_text = build_comment_text(comments, len(references))
    if comment_text:
        parts.append(comment_text)
    prompt = str(prompt or "")
    if prompt:
        parts.append(prompt)
    return parts
