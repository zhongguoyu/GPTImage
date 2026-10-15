# 自托管无限画布

本项目增加可选 Infinite Canvas 静态前端，使用 basketikun/infinite-canvas 官方 v0.20.0 镜像并固定 digest。镜像、源码 commit 和许可证见 `upstream-lock.json`。此组件通过官方镜像部署，未将第三套源码复制到 `sources/`；开发源码可从锁定记录中的仓库和 commit 获取。

## 启动

先按照主 README 初始化并启动 ChatGPT2API 与 Sub2API，再运行：

```bash
docker compose -f compose.yaml -f compose.canvas.yaml up -d infinite-canvas
docker compose -f compose.yaml -f compose.canvas.yaml ps
```

默认只监听宿主机 `127.0.0.1:3002`，可通过 SSH 端口转发预览。生产环境按 `deploy/nginx-canvas.conf.example` 配置 `https://canvas.example.com/`，替换域名与证书后执行 `nginx -t`，成功再 reload。已有反向代理不要整文件覆盖，合并 server 段即可。

也可以使用现有域名的独立 HTTPS 端口，例如 8444：调整 listen、画布 URL 及云平台安全组。只开放所选 TLS 入口，无需开放容器回环端口。应用使用绝对资源路径和前端路由，应部署于独立 origin 的根路径，不应直接放在 `/canvas/` 子路径。

## 接入 ChatGPT2API

1. 在 ChatGPT2API 设置中启用无限画布，将第三方应用 URL 改为自己的 HTTPS 地址。对应配置为 `third_party_apps.infinite_canvas.enabled=true` 和 `third_party_apps.infinite_canvas.url`。
2. 从导航进入无限画布并确认跳转。现有入口通过 `apiKey` 与 `baseUrl` 参数传入当前 ChatGPT2API 调用凭据和公开 API 地址。不要填 Sub2API 管理密钥，也不要把容器内部地址交给浏览器。
3. 首次进入检查画布渠道配置，读取模型并选择可用图片模型。自动导入渠道的模型列表可能为空；模型权限由实际上游决定。
4. 验证生成、图片编辑和 ChatGPT2API 图片管理。完整链路为：画布浏览器 → ChatGPT2API `/v1/images/generations` 或 `/v1/images/edits` → Sub2API → OpenAI。

跨域调用由浏览器发起，ChatGPT2API 的公开地址必须可达且允许画布 origin 的 CORS 预检。HTTPS 画布不能调用 HTTP API。画布自身 Nginx 不代理生成请求，排查 API 失败应查看 ChatGPT2API 与 Sub2API 日志。

## 数据与凭据

画布是静态前端，工作区、渠道和图片保存在浏览器的本地存储 / IndexedDB；部署静态容器不提供云端同步。更换浏览器、清理站点数据或更换 origin 不会自动恢复旧项目。原作者站点的数据不会自动搬到自托管站点，应使用画布自身提供的导出/导入功能迁移所需数据。

首次跳转 URL 含调用凭据，画布读取后会清理地址栏参数。示例反向代理关闭 access log 并设置 no-referrer；额外 CDN、代理和浏览器历史也应避免保存含凭据链接。不要分享这种跳转 URL。Sub2API 专用上游密钥只保存在 ChatGPT2API 服务器私有文件中。

## 故障与更新

- 页面超时：检查 DNS、TLS 入口、云安全组和 Nginx upstream；宿主机容器可达不等于公网已放行。
- 空模型列表：先读取模型并选择；检查 ChatGPT2API `/v1/models` 的认证和上游权限。
- 生图 502 且日志出现旧网页 Cloudflare 403：确认运行的是包含 `api/ai.py` 分流修复的新镜像，且 `IMAGE_UPSTREAM_CONFIG` 指向有效文件。生成、编辑及后台任务必须绕过旧调度器，不能靠更换画布密钥解决。
- Sub2API 401/403：检查密钥和分组权限，不自动回退 ChatGPT 网页。
- 页面有图但图库没有：确认 API baseUrl 指向 ChatGPT2API；直接调用 Sub2API 不经过本项目图片归档。

更新时同时修改 `compose.canvas.yaml` 与 `upstream-lock.json` 的 digest/version/commit，保留原许可证，并验证渠道导入、模型列表、生成、编辑和前端路由。不要把 `latest` 当成已验证版本。运维画布的 Compose 命令都需带上两个 `-f` 参数。
