# 配置说明

## 文件分层

| 文件 | 作用 | 能否提交 |
|---|---|---|
| `.env.example`、`config/*.example.json` | 占位示例 | 可以 |
| `.env` | Compose 凭证和端口 | 不可以 |
| `runtime/chatgpt2api/config.json` | 网页、存储、公开地址 | 不可以 |
| `runtime/chatgpt2api/data/sub2api-image-upstream.json` | 专用上游 API 密钥、key_id、可选 text_model | 不可以 |
| `runtime/chatgpt2api/data/sub2api_config.json` | 管理连接、同步设置 | 不可以 |
| `runtime/sub2api/` | Sub2API 自动生成配置 | 不可以 |

### 图片上游

```json
{
  "base_url": "http://sub2api:8080",
  "api_key": "REPLACE_WITH_DEDICATED_GROUP_KEY",
  "key_id": 0
}
```

实际部署必须填真实密钥和正数 key_id。不要把分组编号硬编码进源码；账户池状态通过密钥所属分组筛选。镜像内环境变量 `IMAGE_UPSTREAM_CONFIG` 指向上述运行文件。

### 存储

默认本地存储，保留期 15 天，日志保留 30 天，沿用集成环境的非敏感运行参数。原项目支持 WebDAV / 双写，可在管理设置中配置。归档失败会使请求失败，排查磁盘、权限、WebDAV 连接；不要把成功返回与成功归档混为一谈。

图片通过 `/images/...` 读取，缩略图通过 `/image-thumbnails/...` 读取。这两条原项目路由不要求登录；图片管理列表需要管理员。若需要私密图片分享，应另行实现签名 URL 或受鉴权的下载流程。

### 模型

本集成补丁识别并透传 `gpt-image-2.5-flare` 和 `gpt-image-2.5-sunburst`，以及原有模型名称。没有将它们悄悄改写成另一模型。最终能力、映射、模型生命周期和请求可用性均由实际 Sub2API / OpenAI 上游决定；不能仅凭 API 回显名称证明后台实际模型身份。

### 子账号

`text_unlimited`、`image_unlimited` 是独立布尔字段。无限制时 API 的 remaining 字段为 null，用量照常累计。管理员创建接口未指定额度时默认无限制；显式提交数字额度且省略 unlimited 字段时，按有限额度处理，兼容旧客户端。

更新已有账号时，应显式提交 unlimited=false 和足以覆盖已用/预留量的限额，才能从无限制切回有限额度。0 始终表示有限模式下无可用次数。已有账号迁移不改变原额度，并发与上游额度不受该开关影响。

### 时间与统计

Compose 默认 TZ=Asia/Shanghai；当前图片状态面板的“今日”口径明确固定为北京时间。若需要其他时区，应同时修改状态聚合逻辑和测试，不能只改容器 TZ。

### 统一上游和无限画布

`SUB2API_ONLY=1` 由主 Compose 设置，禁止回退 ChatGPT 网页接口。历史名称 `IMAGE_UPSTREAM_CONFIG` 现在也供统一文本网关读取 `base_url`、`api_key`；可选 `text_model` 控制 `auto`，默认 `gpt-5.6`。生产应使用符合所需能力的专用分组密钥，权限不足保持原错误，不扩大权限。

可选 `.env` 参数 `INFINITE_CANVAS_PORT` 默认 3002，仅用于画布覆盖文件发布的回环端口。修改端口需同步 Nginx upstream。画布公开 URL 在 ChatGPT2API 第三方应用设置中配置，不写入共享镜像；详见 [画布说明](INFINITE-CANVAS.md)。

### 源码版 Sub2API 的 Google OAuth 凭据

GitHub 推送保护识别到上游快照内置的 Google OAuth 客户端 ID/secret，发布版已移除这些硬编码值。源码构建若使用 Antigravity，需向 Sub2API 容器提供 `ANTIGRAVITY_OAUTH_CLIENT_ID` 与 `ANTIGRAVITY_OAUTH_CLIENT_SECRET`；Gemini CLI 使用 `GEMINI_CLI_OAUTH_CLIENT_ID` 与 `GEMINI_CLI_OAUTH_CLIENT_SECRET`，或管理端支持的自定义 OAuth 客户端配置。请使用自己有权使用的有效客户端，写入私有部署覆盖文件，不提交凭据。

默认 Compose 固定的官方 Sub2API 镜像没有改变，此源码脱敏不会改变该镜像或现有云端服务。OpenAI 生图链路不依赖以上 Google OAuth 凭据。

## 双链路开关

现在可在管理员“设置 → 基础配置 → 上游链路”选择 ChatGPT2API 原生或 Sub2API 网关。显式选择优先于环境变量，旧配置均保留；上文统一网关的行为适用于选择 Sub2API 时。详见 [切换说明](ROUTING-SWITCH.md)。
