# Sub2API 统一上游

GPTImage 的 Compose 默认设置 `SUB2API_ONLY=1`，与已有 `IMAGE_UPSTREAM_CONFIG` 私有文件配合使用。Sub2API 专用上游凭据不进入镜像、日志或浏览器；画布浏览器持有的是用户自己的 ChatGPT2API 调用凭据。普通部署必须保留该环境变量；未设置时仍保留原项目兼容模式。

| 功能 | 上游 |
| --- | --- |
| 模型列表 | Sub2API `/v1/models`，不混入网页模型名称 |
| 文本、视觉输入、函数工具调用 | `/v1/chat/completions`，保留请求参数和返回用量 |
| Responses | `/v1/responses`，保留原生事件和工具调用 |
| Anthropic Messages | `/v1/messages`，遵守当前密钥所属分组的权限；被禁止时返回 403 |
| 生图、编辑、标注、评论 | `/v1/images/generations`、`/v1/images/edits`，仍归档到图片管理 |
| 网页搜索兼容入口 | `/v1/responses` 的 `web_search` 工具，转换回答和引用 |
| 账号状态、额度摘要 | Sub2API 管理接口；未知图片额度仍显示未知 |
| 刷新账号凭据 | Sub2API `/api/v1/admin/openai/accounts/{id}/refresh` |

`auto` 使用私有文件的 `text_model`，未指定时为 `gpt-5.6`。已知旧别名 `gpt-5-6` 映射为 `gpt-5.6`；其他模型不猜测映射，应从模型列表选择。

PPT/PSD 网页文件导出、旧网页任务轮询恢复、ChatGPT 密码登录和 Clearance 在此模式下没有对应能力，明确返回不支持。需在 Sub2API 管理账号授权。所有残留网页客户端在创建时被阻止，不会偷偷回退网页服务。

用户认证、业务额度、并发限制和调用日志仍由 ChatGPT2API 负责；上游账号选择由 Sub2API 负责。本地源码已包含补丁，常规部署使用仓库 Dockerfile 重新构建。上线前备份 Compose 和数据，确认新版本健康；回滚需使用兼容数据格式的旧镜像。

## 双链路开关

现在可在管理员“设置 → 基础配置 → 上游链路”选择 ChatGPT2API 原生或 Sub2API 网关。显式选择优先于环境变量，旧配置均保留；上文统一网关的行为适用于选择 Sub2API 时。详见 [切换说明](ROUTING-SWITCH.md)。
