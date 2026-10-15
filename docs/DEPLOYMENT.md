# 部署指南

所有命令在仓库根目录运行。Windows 使用 Docker Desktop 的 Linux 容器；生产推荐 Linux。Python 脚本只使用标准库。

## 1. 初始化

```bash
python scripts/setup.py init --public-url https://images.example.com --admin-email admin@example.com
```

改成你自己的地址和邮箱。脚本生成随机管理密码、数据库密码、Redis 密码、JWT/TOTP 密钥和 ChatGPT2API 管理密钥，保存于 `.env`，不会在终端打印。POSIX 下文件权限设为 600；Windows 下还应检查所在目录 ACL。不要把 `.env` 内容贴进工单或公开日志。

`runtime/chatgpt2api/config.json` 的 `base_url` 必须是浏览器实际可访问的入口。如果从 localhost 切换到域名，要更新该值并重启 ChatGPT2API，否则新图片地址可能无法被其他设备访问。

## 2. 启动 Sub2API 并配置上游

```bash
docker compose up -d sub2api
docker compose ps
```

等待数据库和应用健康检查通过。使用 `.env` 中的管理员账号登录 Sub2API，添加可用上游账号和分组，为 ChatGPT2API 创建专用密钥。密钥权限、额度及可用模型取决于你的分组设置。本项目不固定分组号，不带任何预置账号。

然后执行 `python scripts/setup.py upstream`，粘贴该密钥并输入其数字 ID。ID 可从 Sub2API 密钥管理返回数据中确认；它不是分组 ID，也不是密钥字符串的一部分。

## 3. 启动 ChatGPT2API

```bash
docker compose up -d --build chatgpt2api
docker compose ps
```

使用 `.env` 的 `CHATGPT2API_AUTH_KEY` 登录。在管理界面的 Sub2API 连接中添加 `http://sub2api:8080`，填写本地部署的管理凭证，按需导入账号。生成转发依赖专用密钥配置；导入账号状态刷新和账户池面板还依赖这条管理连接。

API 密钥配置文件采用读时加载；修改管理连接建议通过界面完成。不要在服务运行时随意编辑数据目录中的账户 JSON。

## 4. 外部访问与 HTTPS

默认两个应用仅绑定宿主机 127.0.0.1。远程调试可使用：

```bash
ssh -L 3000:127.0.0.1:3000 -L 8080:127.0.0.1:8080 user@your-server
```

上线时复制 `deploy/nginx.conf.example`，替换域名和证书路径后使用宿主机 Nginx。示例采用两个域名；若希望同域名不同端口，可让 ChatGPT2API 监听代理入口 8443，并将 `base_url` 改为包含 `:8443` 的 HTTPS 地址。确保防火墙开放对应端口。模板不负责申请证书。

图片响应可能耗时较长，保留禁用代理缓冲及较长读取超时设置。上传限制应同时考虑 Nginx、两个应用和上游的大小上限。

## 5. 可选：构建 Sub2API 源码

```bash
docker compose -f compose.yaml -f compose.source.yaml build sub2api
docker compose -f compose.yaml -f compose.source.yaml up -d
```

后续操作也应使用这两个配置文件，避免不小心切回预构建镜像。Sub2API 原始 Dockerfile 使用 BuildKit 缓存挂载，需要现代 Docker/BuildKit。其构建工具链版本以随附 Dockerfile 为准。

默认部署固定 Sub2API 镜像 digest。PostgreSQL/Redis 固定主版本标签，基础构建镜像仍来自上游 Dockerfile，因此本项目并不保证逐字节可复现构建。

## 验收

- `docker compose ps` 中服务正常。
- 管理员登录两个应用成功。
- 新建子账号显示默认无限制，可改为有限额度。
- 生成和编辑各一张图片，检查结果、图片管理、下载和缩略图。
- 状态面板可显示任务统计；账户池失败时优先核对管理连接和 key_id。
- 对保存任务重启后仍可查询到图片 URL（保留期内文件仍在）。

实际生图验证会消耗你自己的上游额度。仓库测试默认不调用真实上游。

## 可选无限画布

使用独立覆盖文件 `compose.canvas.yaml` 和 HTTPS 模板 `deploy/nginx-canvas.conf.example`；按 [无限画布部署说明](INFINITE-CANVAS.md) 完成公开入口、渠道和模型配置。
