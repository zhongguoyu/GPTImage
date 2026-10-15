"""图片标注（markup）编辑支持。

复刻 chatgpt.com 网页端「标注」画笔编辑（2026-10 抓包确认）：
- 网页端先把笔迹渲染成与原图同尺寸的透明 PNG（仅含笔迹像素，文件名
  image-markup-overlay.png），POST /backend-api/image-gen/markup-preview
  （multipart：original_file_id + markup），由服务端合成并返回 sediment:// 指针；
- 随后的 /backend-api/f/conversation 请求：parts = [合成图指针, 提示词]
  （没有评论编辑那样的坐标文本块，笔迹信息全在像素里），
  metadata.image_gen_flow_tags = ["markup"]，attachments 名为
  image-markup-preview-<file_id>.png，system_hints 为空；
- 编辑会话内已生成图片时还携带 metadata.dalle.from_client.operation
  （type: transformation + original_file_id + original_gen_id + is_markup: true）。
  与评论编辑同样的实测结论：代理的全新会话 + 刚上传图片（没有 original_gen_id）
  不能携带该 metadata，否则上游图片工具直接报错，必须省略；
- 上游响应与普通编辑完全同构：image_gen 工具消息返回新图，
  tool 消息 metadata.dalle.parent_gen_id 指向请求中的 original_gen_id，
  server_ste_metadata.turn_use_case = "image gen"。

代理实现取等价捷径：本地用 PIL 把笔迹 overlay 直接合成到原图上再按普通
编辑图上传（上游模型看到的输入与网页端服务端合成本一致——都是带笔迹的位图），
并在 metadata 中携带 image_gen_flow_tags=["markup"] 对齐网页端标记。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from io import BytesIO
from typing import Any

from PIL import Image, ImageDraw

# 笔迹默认颜色与上限（与网页端一致：默认红色画笔）
DEFAULT_MARKUP_COLOR = "#FF3B30"
MAX_MARKUP_STROKES = 200
MAX_MARKUP_POINTS_PER_STROKE = 2000
# 相对线宽（相对图片短边的比例）的上下限
MIN_STROKE_WIDTH_RATIO = 0.0005
MAX_STROKE_WIDTH_RATIO = 0.25
DEFAULT_STROKE_WIDTH_RATIO = 0.008
# 抗锯齿超采样倍数
_OVERLAY_SUPERSAMPLE = 4


@dataclass
class MarkupStroke:
    """一条笔迹：points 为图片内归一化 [0,1] 坐标，color 为 RGB 元组，
    width 为相对图片短边的线宽比例。"""

    points: list[tuple[float, float]]
    color: tuple[int, int, int] = (255, 59, 48)
    width: float = DEFAULT_STROKE_WIDTH_RATIO


def _parse_markup_coordinate(value: Any, field_name: str) -> float:
    """解析笔迹坐标：接受 [0,1] 数值、"27.5%" 或 0-100 的数值（与评论坐标同语义）。"""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"markup stroke {field_name} must be a number between 0 and 1, a percentage string, or 0-100")
    percent = False
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"markup stroke {field_name} must not be empty")
        if text.endswith("%"):
            percent = True
            text = text[:-1].strip()
        try:
            number = float(text)
        except ValueError as exc:
            raise ValueError(
                f"markup stroke {field_name} must be a number between 0 and 1, a percentage string, or 0-100"
            ) from exc
    elif isinstance(value, (int, float)):
        number = float(value)
    else:
        raise ValueError(
            f"markup stroke {field_name} must be a number between 0 and 1, a percentage string, or 0-100"
        )
    if percent or number > 1.0:
        number = number / 100.0
    if not 0.0 <= number <= 1.0:
        raise ValueError(f"markup stroke {field_name} must be within the image (0-100%)")
    return number


def _parse_markup_color(value: Any) -> tuple[int, int, int]:
    """解析笔迹颜色：接受 #RGB / #RRGGBB / "rgb(r,g,b)" / [r,g,b]。"""
    if value is None or (isinstance(value, str) and not value.strip()):
        return (255, 59, 48)
    if isinstance(value, (list, tuple)):
        if len(value) != 3:
            raise ValueError("markup stroke color must have exactly 3 components (r, g, b)")
        try:
            channels = [int(round(float(component))) for component in value]
        except (TypeError, ValueError) as exc:
            raise ValueError("markup stroke color components must be numbers") from exc
    elif isinstance(value, str):
        text = value.strip().lower()
        rgb_match = None
        if text.startswith("rgb(") and text.endswith(")"):
            rgb_match = text[4:-1].split(",")
        if rgb_match is not None:
            if len(rgb_match) != 3:
                raise ValueError("markup stroke color must have exactly 3 components (r, g, b)")
            try:
                channels = [int(round(float(component.strip()))) for component in rgb_match]
            except (TypeError, ValueError) as exc:
                raise ValueError("markup stroke color components must be numbers") from exc
        else:
            hex_text = text.removeprefix("#")
            if len(hex_text) == 3:
                hex_text = "".join(ch * 2 for ch in hex_text)
            if len(hex_text) != 6 or any(ch not in "0123456789abcdef" for ch in hex_text):
                raise ValueError("markup stroke color must be #RGB, #RRGGBB, rgb(r,g,b) or [r,g,b]")
            channels = [int(hex_text[index:index + 2], 16) for index in (0, 2, 4)]
    else:
        raise ValueError("markup stroke color must be #RGB, #RRGGBB, rgb(r,g,b) or [r,g,b]")
    if any(channel < 0 or channel > 255 for channel in channels):
        raise ValueError("markup stroke color components must be within 0-255")
    return (channels[0], channels[1], channels[2])


def _parse_stroke_width(value: Any) -> float:
    """解析相对线宽：接受 (0,1] 比例、百分比字符串或 0-100 数值。"""
    if value is None or value == "":
        return DEFAULT_STROKE_WIDTH_RATIO
    if isinstance(value, bool):
        raise ValueError("markup stroke width must be a positive ratio of the image short side")
    percent = False
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return DEFAULT_STROKE_WIDTH_RATIO
        if text.endswith("%"):
            percent = True
            text = text[:-1].strip()
        try:
            number = float(text)
        except ValueError as exc:
            raise ValueError("markup stroke width must be a positive ratio of the image short side") from exc
    elif isinstance(value, (int, float)):
        number = float(value)
    else:
        raise ValueError("markup stroke width must be a positive ratio of the image short side")
    if percent or number > 1.0:
        number = number / 100.0
    if not MIN_STROKE_WIDTH_RATIO <= number <= MAX_STROKE_WIDTH_RATIO:
        raise ValueError(
            f"markup stroke width must be between {MIN_STROKE_WIDTH_RATIO} and {MAX_STROKE_WIDTH_RATIO} "
            "(ratio of the image short side)"
        )
    return number


def normalize_markup_strokes(value: Any) -> list[MarkupStroke]:
    """归一化笔迹列表：接受 JSON 字符串、dict 列表或已归一化列表，非法输入抛 ValueError。

    每条笔迹：{"points": [[x, y], ...], "color": "#FF3B30"（可选）, "width": 0.008（可选）}。
    """
    if value is None or value == [] or value == "":
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("markup_strokes must be valid JSON") from exc
    if not isinstance(value, list):
        raise ValueError("markup_strokes must be a list")
    strokes: list[MarkupStroke] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("each markup stroke must be an object with points")
        raw_points = item.get("points")
        if raw_points is None:
            raw_points = item.get("path")
        if not isinstance(raw_points, list) or not raw_points:
            raise ValueError("markup stroke points must be a non-empty list of [x, y] pairs")
        if len(raw_points) > MAX_MARKUP_POINTS_PER_STROKE:
            raise ValueError(f"markup stroke has too many points (max {MAX_MARKUP_POINTS_PER_STROKE})")
        points: list[tuple[float, float]] = []
        for raw_point in raw_points:
            if isinstance(raw_point, dict):
                raw_x = raw_point.get("x")
                raw_y = raw_point.get("y")
            elif isinstance(raw_point, (list, tuple)) and len(raw_point) == 2:
                raw_x, raw_y = raw_point[0], raw_point[1]
            else:
                raise ValueError("each markup stroke point must be an [x, y] pair or {x, y} object")
            x = _parse_markup_coordinate(raw_x, "point x")
            y = _parse_markup_coordinate(raw_y, "point y")
            points.append((x, y))
        strokes.append(
            MarkupStroke(
                points=points,
                color=_parse_markup_color(item.get("color")),
                width=_parse_stroke_width(item.get("width")),
            )
        )
    if len(strokes) > MAX_MARKUP_STROKES:
        raise ValueError(f"too many markup strokes (max {MAX_MARKUP_STROKES})")
    return strokes


def markup_strokes_to_dicts(strokes: list[MarkupStroke]) -> list[dict[str, Any]]:
    """转成可 JSON 持久化的 dict 列表（任务状态存储用）。"""
    return [
        {
            "points": [[round(x, 6), round(y, 6)] for x, y in stroke.points],
            "color": "#%02X%02X%02X" % stroke.color,
            "width": stroke.width,
        }
        for stroke in strokes
    ]


def render_markup_overlay(strokes: list[MarkupStroke], width: int, height: int) -> bytes:
    """把笔迹渲染成与目标尺寸一致的透明 PNG overlay（仅含笔迹像素）。"""
    if width <= 0 or height <= 0:
        raise ValueError("markup overlay size must be positive")
    scale = _OVERLAY_SUPERSAMPLE
    big_width, big_height = width * scale, height * scale
    # 超采样尺寸过大时回退到 1x，避免超大图内存膨胀
    if big_width * big_height > 64_000_000:
        scale = 1
        big_width, big_height = width, height
    overlay = Image.new("RGBA", (big_width, big_height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    short_side = min(big_width, big_height)
    for stroke in strokes:
        pixel_points = [
            (min(big_width - 1, x * big_width), min(big_height - 1, y * big_height))
            for x, y in stroke.points
        ]
        radius = max(1, round(stroke.width * short_side / 2))
        fill = stroke.color + (255,)
        unique_points = [pixel_points[0]]
        for point in pixel_points[1:]:
            if point != unique_points[-1]:
                unique_points.append(point)
        if len(unique_points) == 1:
            cx, cy = unique_points[0]
            draw.ellipse([cx - radius, cy - radius, cx + radius, cy + radius], fill=fill)
            continue
        draw.line(unique_points, fill=fill, width=radius * 2, joint="curve")
        # round caps：两端补圆
        for endpoint in (unique_points[0], unique_points[-1]):
            cx, cy = endpoint
            draw.ellipse([cx - radius, cy - radius, cx + radius, cy + radius], fill=fill)
    result = overlay.resize((width, height), Image.LANCZOS)
    try:
        buffer = BytesIO()
        result.save(buffer, format="PNG")
        return buffer.getvalue()
    finally:
        overlay.close()
        result.close()


def composite_markup_overlay(
    image_bytes: bytes,
    overlay_bytes: bytes | None = None,
    strokes: list[MarkupStroke] | None = None,
) -> bytes:
    """把 overlay（显式提供）或笔迹（现场渲染）合成到原图，返回 PNG 字节。"""
    with Image.open(BytesIO(image_bytes)) as source:
        source_image = source.convert("RGBA")
    try:
        if overlay_bytes is not None:
            with Image.open(BytesIO(overlay_bytes)) as overlay_image:
                overlay = overlay_image.convert("RGBA")
        else:
            overlay_bytes_rendered = render_markup_overlay(
                strokes or [], source_image.width, source_image.height
            )
            with Image.open(BytesIO(overlay_bytes_rendered)) as overlay_image:
                overlay = overlay_image.convert("RGBA")
        try:
            if overlay.size != source_image.size:
                overlay = overlay.resize(source_image.size, Image.LANCZOS)
            composited = Image.alpha_composite(source_image, overlay)
            try:
                buffer = BytesIO()
                composited.save(buffer, format="PNG")
                return buffer.getvalue()
            finally:
                composited.close()
        finally:
            overlay.close()
    finally:
        source_image.close()


def composite_markup_images(
    images: list[tuple[bytes, str, str]],
    overlays: list[tuple[bytes, str, str]] | None,
    strokes: list[MarkupStroke] | None,
) -> list[tuple[bytes, str, str]]:
    """把 markup overlay/笔迹合成进每张输入图。

    - overlays[i] 缺失时回退用 strokes 现场渲染；
    - 两者都为空时原样返回；
    - 合成结果统一编码为 PNG（笔迹信息在像素里，与网页端合成图一致）。
    """
    overlays = overlays or []
    strokes = strokes or []
    if not overlays and not strokes:
        return images
    result: list[tuple[bytes, str, str]] = []
    for index, (data, filename, mime_type) in enumerate(images):
        overlay_bytes = overlays[index][0] if index < len(overlays) else (overlays[-1][0] if overlays else None)
        composited = composite_markup_overlay(data, overlay_bytes, strokes)
        safe_name = str(filename or "image.png")
        if not safe_name.lower().endswith(".png"):
            base = safe_name.rsplit(".", 1)[0] if "." in safe_name else safe_name
            safe_name = f"{base}.png"
        result.append((composited, safe_name, "image/png"))
    return result
