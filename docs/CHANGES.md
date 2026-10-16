# 集成修改清单

基线和来源见 `upstream-lock.json`。本项目不复制生产配置，只保留代码行为。

| 模块 | 修改 |
|---|---|
| `services/protocol/image_upstream.py` | 标准图片生成/编辑转发；私有文件读取密钥；模型透传；保存结果和图库索引 |
| `openai_v1_image_generations.py` / `openai_v1_image_edit.py` | 配置存在时接入 Sub2API 适配器；保留原处理路径 |
| `services/sub2api_account_info.py` / `account_service.py` | 严格模式下所有匹配账号从 Sub2API 刷新元信息与凭据，不依赖网页接口 |
| `services/image_operational_status.py` / 状态面板 | 任务统计、账户池、冷却、Codex 使用窗口；不推算图片张数 |
| `services/quota_service.py` / `api/accounts.py` / 用户管理 UI | 新子账号默认无限制，独立 flags，继续用量结算，兼容已有额度 |
| `utils/helper.py` / 模型目录 | 新模型名称识别；结构化错误和网页挑战错误处理 |
| `services/protocol/conversation.py` | 配置上游时走图片适配器；旧认证图片下载仅留于非严格兼容路径 |
| `services/scheduler/account_pool.py` | 修正失败账号因低延迟反而获得更优评分的问题 |
| `pyproject.toml` / `uv.lock` / `.dockerignore` | 修正 mini-racer 版本和构建资产遗漏 |
| `test/` | 覆盖接入、编辑、模型、状态、额度及归档回归 |

根目录新增：统一 Compose、脱敏配置模板、初始化工具、发布扫描、工具测试及中文文档。

## 保留的边界

- Sub2API 源码以固定上游版本快照保留；发布时移除内置 Google OAuth 凭据，改由部署环境提供。
- 已增加严格 Sub2API 模式：文本、Responses、Messages、模型列表与搜索统一路由；无对应接口的旧网页能力明确报错。
- 配置生图上游即绕过旧账号调度器，覆盖生成、编辑及后台任务，修复无限画布请求误入网页接口的问题。
- 标准图片适配器和原生 Responses 图片输出均归档；严格模式禁止旧网页回退。
- 旧任务中没有保存的图片字节不可能由这份代码自动重建。
- 模型名称透传不提供上游模型身份的独立证明。

## 后续整合：2026-10-15

| 文件 / 模块 | 变化 |
|---|---|
| `services/sub2_gateway.py` | Sub2API 请求、SSE、模型别名、搜索和原生 Responses 图片归档 |
| `api/ai.py`、`services/image_task_service.py` | 生成/编辑/任务优先分流，避免旧账号调度器接管 |
| `services/protocol/` 的 Chat、Responses、Messages、Models、Search | 转发对应 Sub2API 路由，保留权限错误 |
| `services/openai_backend_api.py`、`editable_file_task_service.py`、`proxy_service.py` | 严格模式阻止网页客户端及不支持能力 |
| `utils/helper.py` | Responses SSE 事件类型兼容 |
| `test/test_sub2_gateway.py` | 路由、流式、工具调用与禁用网页回退回归 |
| `web/src/app/globals.css`、公共布局及业务页 | 深色控制台主题、琥珀强调色、移动端导航；管理员生图页移除顶部状态面板 |
| `compose.yaml` | 默认启用 `SUB2API_ONLY=1` |
| `compose.canvas.yaml`、`deploy/nginx-canvas.conf.example` | 自托管无限画布官方固定镜像、回环端口、独立 HTTPS origin |
| `upstream-lock.json`、许可证、中文文档 | 记录画布来源、部署接入、验证范围及功能限制 |

本地发布模板以占位域名和新部署路径组织，与现有服务器的私有 Compose 路径不同；不复制现网凭据或运行数据。

## 2026-10-16：双链路切换

新增 `services/routing_mode.py`、管理员 `/api/routing-mode` 和设置卡片；统一控制严格模式、图片分流与账号元信息，保留原生实现。请求/任务执行期间禁止切换，选择持久化。补齐原生图片归档和缓存清理，新增路由和权限回归。

## 原作者原生链路接入

新增锁定的 `services/original_web_backend.py`，通过兼容子类替换原生网页客户端的请求实现，保留 fork 调度器、任务、UI、图片存储、子账号额度和 Sub2API 路由。恢复原作者的账号刷新及文本/图片请求流程；新增 `test/test_original_transport.py` 验证指纹、会话入口、评论兼容、异常类型及网关隔离。


## 账户信息按上游链路同步

切换链路自动刷新现有账号并返回成功／失败数量。账号信息带来源及更新时间，跨链路旧数据和刷新失败数据显示待刷新；原生显示明确返回的生图额度，Sub2API 显示网关用量窗口，未知生图额度不再混用原生数值。保留子账号业务授权与累计统计。详见 ROUTING-SWITCH.md 与 VALIDATION.md。
