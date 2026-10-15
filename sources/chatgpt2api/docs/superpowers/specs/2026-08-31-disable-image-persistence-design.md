# 图片生成无持久化设计

## 背景

项目作为上游 API 为下游提供图片生成和图片编辑能力。当前上游返回图片 bytes 后，`services/protocol/conversation.py` 中的 `format_image_result()` 会调用 `ImageStorageService.save()`，把图片写入 `data/images/`，并更新 `data/image_index.json`。异步图片任务还会把格式化后的结果写入 `data/image_tasks.json`。

下游主要使用 `POST /v1/images/generations` 和 `POST /v1/images/edits`，且请求示例不传 `response_format`，因此使用默认的 `b64_json` 响应。需求是取消生成过程中的服务端图片持久化，同时保持图片生成和编辑结果可正常返回。

## 目标

- 图片生成和图片编辑结果只在请求处理期间以内存形式传递，不再写入服务端图片目录。
- 不再因生成结果创建或更新 `data/image_index.json`。
- 异步图片任务仍能在当前进程内返回结果，但任务持久化文件不保存图片内容。
- 默认 `b64_json` API 调用保持可用，并保留图片数据和 `revised_prompt`。
- 显式 `response_format="url"` 的请求仍返回 `url` 字段，但该字段使用当前响应内的 `data:` URL，不提供持久 HTTP 地址。
- 保留已有图片管理、历史图片读取、删除和 WebDAV 管理能力，避免删除超出需求范围的既有功能。

## 非目标

- 不自动删除已经存在的 `data/images/` 图片、`image_index.json` 条目或旧任务数据。
- 不改变上游 ChatGPT 图片轮询、账号轮换、重试、内容策略错误和超时处理逻辑。
- 不改变浏览器端 LocalForage 对图片会话历史的存储行为；该存储属于客户端功能，不是服务端生成落盘链路。

## 设计

### 结果格式化

移除 `services/protocol/conversation.py` 对 `image_storage_service` 和 `save_image_bytes()` 的依赖。`format_image_result()` 继续接收上游下载得到的 Base64 图片项，但改为：

- `response_format == "b64_json"`：每项返回 `b64_json` 和 `revised_prompt`。
- 其他格式（当前支持路径为 `url`）：每项返回 `url: "data:image/png;base64,<base64>"` 和 `revised_prompt`。

结果格式化不再计算图片存储路径、不再调用本地文件 API、不再更新图片索引。图片 bytes 仍会在内存中被 Base64 编码并通过 HTTP/SSE 返回，这是 API 响应本身所必需的短期数据传递。

### 异步任务持久化

`ImageTaskService` 保持现有线程、状态机、轮询和当前进程内任务字典不变。任务成功后，`data` 仍写入内存中的任务对象，使当前进程内的任务查询和在线画图页面可以显示图片。

任务写入 `image_tasks.json` 时只保留任务状态和元数据，不序列化生成结果中的图片内容。重新启动服务后，已完成任务仍可查询状态、模型、时间、用量等元数据，但不会恢复图片结果；这符合不在服务端持久化图片的目标。已有文件不主动清理。

### 现有存储功能

`ImageStorageService`、图片管理路由、缩略图、下载、清理和 WebDAV 配置继续保留，用于管理已有图片或显式存储操作。生成结果路径不再调用该服务，因此默认图片生成不会新增本地文件、索引项或 WebDAV 上传。

## 数据流

```text
上游图片 URL
    -> 内存 bytes
    -> Base64
    -> format_image_result()
       -> b64_json 响应：b64_json
       -> url 响应：data:image/png;base64,...
    -> API/SSE/异步任务当前进程结果
```

`ImageStorageService.save()` 和 `data/images/` 不再位于生成结果数据流中。异步任务仍会持久化任务状态，但持久化前会移除图片结果数据。

## 兼容性与错误处理

- 不传 `response_format` 的生成和编辑请求继续默认返回 `b64_json`。
- `n`、流式响应、编辑输入、Chat Completions、Responses 和 Codex 图片链路继续复用同一结果格式化逻辑。
- `b64_json` 响应不再附带由服务端生成的本地 `url` 字段，符合 Base64 响应的标准语义。
- `url` 响应的 `data:` URL 仅在当前响应中有效，调用方如果需要长期或独立 HTTP 地址，应使用 `b64_json` 并自行管理图片。
- Base64 解码、上游无图片、轮询超时、账号不可用等错误保持现有处理方式。
- 存储目录权限、图片索引写入失败和 WebDAV 连接失败不再成为图片生成成功的前置条件。

## 验证

新增或调整后端单元测试：

1. `format_image_result()` 在 `b64_json` 模式下返回 Base64 和修订提示，不返回 `url`，且图片存储服务没有被调用。
2. `format_image_result()` 在 `url` 模式下返回 `data:image/png;base64,...`，且不会创建本地图片文件或索引。
3. 异步任务在当前进程内仍返回图片数据，但写入任务文件的内容不包含图片 Base64 或其他图片内容；重新实例化服务后仍能读取任务状态和元数据。
4. 保留并运行现有图片存储服务测试，确认历史图片管理和 WebDAV 行为没有被意外删除。

验证顺序遵循 TDD：先运行新增测试确认因旧行为失败，再实现最小修改，最后运行完整 Python 测试集。
