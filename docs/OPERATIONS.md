# 运维与排障

## 查看状态

```bash
docker compose ps
docker compose logs --tail 100 chatgpt2api
docker compose logs --tail 100 sub2api
```

日志可能包含请求内容或账号信息，提交问题前请脱敏。不要公开 `docker compose config` 的完整输出，它会展开 `.env` 的秘密值。

## 备份

需要一起备份 `.env`、`runtime/`、PostgreSQL、Redis。它们含管理凭证和用户数据，备份目录不能提交 Git。

一致性最容易保证的流程是维护窗口中停止两个应用，保存 PostgreSQL 的逻辑备份并复制配置和 ChatGPT2API 数据。不要直接复制仍在写入的 SQLite 主文件而遗漏 WAL。下面命令用于 Linux shell：

```bash
mkdir -p backups
chmod 700 backups
docker compose stop chatgpt2api sub2api
docker compose exec -T postgres pg_dump -U sub2api -d sub2api -Fc > backups/sub2api.dump
tar -czf backups/runtime.tar.gz runtime .env
docker compose exec -T redis sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli SAVE'
docker compose stop redis
docker run --rm --volumes-from "$(docker compose ps -a -q redis)" alpine:3.21 tar -C /data -czf - . > backups/redis.tar.gz
docker compose start redis sub2api chatgpt2api
```

以上包含 Redis 停机卷备份。运行目录和卷恢复时应保留所有权与权限；建议使用专用备份系统管理加密和保留周期。恢复时使用与备份匹配的应用和数据库版本，并先在隔离环境演练。

## 升级与回滚

先备份，确认没有排队/运行中的图片任务，再更新镜像。保存升级前的 Git commit、Compose、镜像 digest 和数据备份。应用回滚不等于数据库迁移回滚，必须确认旧版本兼容新增字段；不要不经检查直接用旧版本解释新子账号额度。

```bash
docker compose build chatgpt2api
docker compose up -d --no-deps chatgpt2api
```

如果采用源码版 Sub2API，升级命令需带 `-f compose.yaml -f compose.source.yaml`。

## 常见问题

| 现象 | 优先检查 |
|---|---|
| 网页有图，图片管理没有 | 是否经过本集成图片适配器；是否旧版本尚未启用归档；存储/索引是否写成功 |
| 图片地址只有本机能访问 | config.json 的 base_url 是否仍为 localhost；代理端口/证书是否正确 |
| /conversation/init 403 | 是否进入旧网页刷新路径；严格模式是否启用、新镜像是否生效、Sub2API 管理连接和上游配置是否匹配 |
| 图片上游 HTTP 401/403 | 专用密钥是否有效、所属分组权限及账号状态 |
| 404 或模型不可用 | Sub2API 版本、路由及实际上游支持；不能只增加前端模型选项 |
| 429 | 上游限流、冷却、业务额度或并发；根据错误来源区分 |
| 账户池状态不可获取 | 管理凭证、内部 base_url 一致性、key_id 和管理接口兼容性 |
| 生图剩余张数显示未知 | 正常：当前没有可靠的官方张数数据，不用 Codex 百分比推算 |
| 升级前的成功任务没有图片 | 旧 fork 没保存图片字节，服务器任务元数据无法还原；检查原浏览器历史 |
| 无限额度仍不能请求 | 检查独立并发、上游分组、上游余额/冷却和账号启用状态 |

图库原图 URL 无登录校验，保留期到期、手动删除或存储清理后，历史 URL 可能失效；“持久地址”不代表永久保存。

## 自托管画布与旧网页错误

画布启动、日志、更新命令使用 `docker compose -f compose.yaml -f compose.canvas.yaml`。画布容器仅提供静态文件，API 错误需检查 ChatGPT2API / Sub2API；浏览器项目数据需单独导出备份，服务器卷备份不包括浏览器数据。

新部署若仍出现 `/backend-api/`、`conversation/init` 或 Cloudflare 网页挑战错误，应先核对运行镜像是否包含统一路由补丁、`SUB2API_ONLY=1` 是否生效、`IMAGE_UPSTREAM_CONFIG` 是否存在。尤其检查生成/编辑接口是否绕过旧账号调度器。不要通过关闭严格模式掩盖配置问题。详细步骤见 [无限画布](INFINITE-CANVAS.md)。
