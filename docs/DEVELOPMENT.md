# 开发与验证

## 源码布局

两套源码以快照形式保留在 `sources/`，不是 git submodule。普通 clone 即可拿到可编辑源码，不需要访问原部署服务器。未导入上游 Git 历史。确切来源见根目录 `upstream-lock.json`；原项目的 README、许可证和开发文档保留在各自目录中，根目录文档描述本集成行为，可能与原 fork 的旧设计不同。

## 本地工具检查

```bash
python -m unittest discover -s tests -v
python scripts/check_release.py
docker compose --env-file .env.example config --quiet
```

最后一条只检查 Compose，不可用示例占位密码启动生产服务。发布检查脚本扫描 Git 选定文件；初始化 Git 后先 `git add`，再执行检查。运行目录不属于发布内容。

## ChatGPT2API

```bash
cd sources/chatgpt2api
uv sync --frozen
```

运行后端和测试需要非空 `CHATGPT2API_AUTH_KEY`；本地用临时测试值，不要使用生产密钥。依赖按 `uv.lock` 锁定。部署构建使用仓库 Dockerfile。

针对集成改动的隔离测试（从 ChatGPT2API 目录运行）：

```bash
uv run --with pytest --with httpx python -m pytest -q \
  test/test_original_transport.py test/test_routing_mode.py test/test_sub2_gateway.py test/test_upstream_gallery.py test/test_sub2_image_upstream.py \
  test/test_sub2_complete_routes.py test/test_image25_routes.py \
  test/test_image_operational_status.py test/test_unlimited_quota.py \
  test/test_quota_service.py test/test_quota_api.py \
  test/test_quota_request_integration.py test/test_image_storage_service.py
```

测试使用模拟上游和临时存储；不要挂载生产数据。原仓库 Next 构建跳过类型检查，前端改动后必须另外执行：

```bash
cd web
npm install
npx tsc --noEmit --pretty false
npm run build
```

## Sub2API

Sub2API 源码仅增加发布脱敏修改，移除内置 Google OAuth 凭据。需要开发时参考 `sources/sub2api/README.md`、`backend/go.mod`、`frontend/package.json` 和原项目开发文档。完整构建可使用根目录 `compose.source.yaml`。后端 Go 与前端 pnpm 版本以锁定快照为准。

## 升级上游

1. 在独立分支保存当前集成版本，备份运行数据。
2. 从可信上游获取目标版本，在独立目录比较源码，不直接覆盖 `runtime/`。
3. 对照 `docs/CHANGES.md` 迁移 ChatGPT2API 本地补丁。
4. 更新 `upstream-lock.json` 和 Compose 中镜像 digest / 构建版本。
5. 运行回归、前端类型检查及构建，再在隔离部署中验证生成、编辑、归档和有限额度。
6. 更新许可证说明、变更记录和发布检查报告。

不要把 Sub2API `latest` 漂移当作兼容性保证。本集成使用了 Sub2API 管理端接口及内部字段；这些字段升级后可能改变。

画布覆盖配置检查：`docker compose --env-file .env.example -f compose.yaml -f compose.canvas.yaml config --quiet`。此命令只校验配置，不启动使用占位凭据的服务。
