# User Key Web Login Permission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为每个用户密钥增加可编辑的 Web 登录权限，关闭时只禁止普通用户 Web 登录，不影响启用状态下的 API 鉴权，并部署验证到 `localhost:3000`。

**Architecture:** 保留 `AuthService.authenticate()` 的通用 API 鉴权语义，新增 `authenticate_web_login()` 并只由 `/auth/login` 使用。用户密钥记录以读时默认值兼容历史数据，管理接口和设置页通过 `web_login_enabled` 读写；全局 `enabled` 仍独立控制所有用途。前端沿用现有用户密钥编辑对话框和 Radix `Checkbox`，不引入新的 UI 依赖。

**Tech Stack:** Python 3.13、FastAPI、Pydantic、unittest、Next.js 16、React 19、TypeScript、Radix Checkbox、Docker。

---

## 文件结构与职责

- Modify: `services/auth_service.py` - 归一化、公开返回、更新和 Web 专用认证逻辑。
- Modify: `api/support.py` - 增加只供 Web 登录使用的身份校验入口。
- Modify: `api/system.py` - `/auth/login` 切换到 Web 专用校验。
- Modify: `api/accounts.py` - 接收和更新用户密钥的 `web_login_enabled` 字段。
- Modify: `web/src/lib/api.ts` - 补充前端用户密钥类型和更新请求类型。
- Modify: `web/src/app/settings/components/user-keys-card.tsx` - 展示 Web 登录状态并在编辑对话框中修改。
- Modify: `test/test_account_image_capabilities.py` - 验证认证服务的默认值、独立权限和管理员行为。
- Modify: `test/test_quota_api.py` - 验证用户密钥 API 能读写 Web 登录字段。
- Modify: `test/test_system_api.py` - 验证 Web 登录路由使用专用身份校验。

## Task 1: 写认证服务的失败测试

**Files:**
- Modify: `test/test_account_image_capabilities.py:200-272`

- [ ] **Step 1: 添加历史记录默认允许 Web 登录的测试**

在 `AuthServiceTests` 中加入：

```python
    def test_legacy_user_keys_default_to_web_login_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            storage = JSONStorageBackend(Path(tmp_dir) / "accounts.json", Path(tmp_dir) / "auth_keys.json")
            service = AuthService(storage)
            _item, raw_key = service.create_key(role="user", name="Legacy")

            records = storage.load_auth_keys()
            records[0].pop("web_login_enabled", None)
            storage.save_auth_keys(records)

            reloaded = AuthService(storage)

            listed = reloaded.list_keys(role="user")
            self.assertTrue(listed[0]["web_login_enabled"])
            self.assertIsNotNone(reloaded.authenticate_web_login(raw_key))
```

- [ ] **Step 2: 添加 Web 登录权限不影响 API 鉴权的测试**

```python
    def test_web_login_permission_does_not_disable_api_authentication(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AuthService(JSONStorageBackend(Path(tmp_dir) / "accounts.json", Path(tmp_dir) / "auth_keys.json"))
            item, raw_key = service.create_key(role="user", name="Alice")

            updated = service.update_key(item["id"], {"web_login_enabled": False}, role="user")

            self.assertIsNotNone(updated)
            self.assertFalse(updated["web_login_enabled"])
            self.assertIsNone(service.authenticate_web_login(raw_key))
            self.assertIsNotNone(service.authenticate(raw_key))
```

- [ ] **Step 3: 添加管理员忽略用户 Web 登录字段的测试**

```python
    def test_admin_authentication_ignores_web_login_permission(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AuthService(JSONStorageBackend(Path(tmp_dir) / "accounts.json", Path(tmp_dir) / "auth_keys.json"))
            item, raw_key = service.create_key(role="admin", name="Admin")

            updated = service.update_key(item["id"], {"web_login_enabled": False}, role="admin")

            self.assertIsNotNone(updated)
            self.assertNotIn("web_login_enabled", updated)
            self.assertIsNotNone(service.authenticate_web_login(raw_key))
```

- [ ] **Step 4: 运行测试确认当前实现按预期失败**

```bash
python3 -m unittest \
  test.test_account_image_capabilities.AuthServiceTests.test_legacy_user_keys_default_to_web_login_enabled \
  test.test_account_image_capabilities.AuthServiceTests.test_web_login_permission_does_not_disable_api_authentication \
  test.test_account_image_capabilities.AuthServiceTests.test_admin_authentication_ignores_web_login_permission
```

Expected: FAIL，原因是当前 `AuthService` 没有 `web_login_enabled` 公开字段和 `authenticate_web_login()` 方法，而不是测试收集失败。

- [ ] **Step 5: 提交失败测试**

```bash
git add test/test_account_image_capabilities.py
git commit -m "test: define user key web login behavior"
```

## Task 2: 实现认证服务的独立 Web 登录权限

**Files:**
- Modify: `services/auth_service.py:40-61,78-87,150-172,174-206,225-250`

- [ ] **Step 1: 在归一化和公开返回中加入用户字段**

在 `_normalize_item()` 返回值中加入：

```python
            "web_login_enabled": bool(raw.get("web_login_enabled", True)),
```

将 `_public_item()` 改为只向用户密钥公开该字段：

```python
    @staticmethod
    def _public_item(item: dict[str, object]) -> dict[str, object]:
        public = {
            "id": item.get("id"),
            "name": item.get("name"),
            "role": item.get("role"),
            "enabled": bool(item.get("enabled", True)),
            "created_at": item.get("created_at"),
            "last_used_at": item.get("last_used_at"),
        }
        if item.get("role") == "user":
            public["web_login_enabled"] = bool(item.get("web_login_enabled", True))
        return public
```

- [ ] **Step 2: 创建用户密钥时显式写入默认值**

在 `create_key()` 的 `item` 字典中加入：

```python
                "enabled": True,
                "web_login_enabled": True,
```

- [ ] **Step 3: 允许仅用户角色更新 Web 登录开关**

在 `update_key()` 计算 `next_role` 后、保存前加入：

```python
                if next_role == "user" and "web_login_enabled" in updates and updates.get("web_login_enabled") is not None:
                    next_item["web_login_enabled"] = bool(updates.get("web_login_enabled"))
```

- [ ] **Step 4: 抽取认证公共逻辑并增加 Web 专用入口**

用下面的实现替换现有 `authenticate()`，让 Web 权限检查发生在更新 `last_used_at` 之前：

```python
    def _authenticate(self, raw_key: str, *, require_web_login: bool = False) -> dict[str, object] | None:
        candidate = self._clean(raw_key)
        if not candidate:
            return None
        candidate_hash = _hash_key(candidate)
        with self._lock:
            for index, item in enumerate(self._items):
                if not bool(item.get("enabled", True)):
                    continue
                stored_hash = self._clean(item.get("key_hash"))
                if not stored_hash or not hmac.compare_digest(stored_hash, candidate_hash):
                    continue
                if require_web_login and item.get("role") == "user" and not bool(item.get("web_login_enabled", True)):
                    return None
                next_item = dict(item)
                now = datetime.now(timezone.utc)
                next_item["last_used_at"] = now.isoformat()
                self._items[index] = next_item
                item_id = self._clean(next_item.get("id"))
                last_flush_at = self._last_used_flush_at.get(item_id)
                if last_flush_at is None or (now - last_flush_at).total_seconds() >= 60:
                    try:
                        self._save()
                        self._last_used_flush_at[item_id] = now
                    except Exception:
                        pass
                return self._public_item(next_item)
        return None

    def authenticate(self, raw_key: str) -> dict[str, object] | None:
        return self._authenticate(raw_key)

    def authenticate_web_login(self, raw_key: str) -> dict[str, object] | None:
        return self._authenticate(raw_key, require_web_login=True)
```

- [ ] **Step 5: 运行认证服务测试确认通过**

```bash
python3 -m unittest \
  test.test_account_image_capabilities.AuthServiceTests.test_legacy_user_keys_default_to_web_login_enabled \
  test.test_account_image_capabilities.AuthServiceTests.test_web_login_permission_does_not_disable_api_authentication \
  test.test_account_image_capabilities.AuthServiceTests.test_admin_authentication_ignores_web_login_permission \
  test.test_account_image_capabilities.AuthServiceTests.test_create_authenticate_disable_and_delete_user_key \
  test.test_account_image_capabilities.AuthServiceTests.test_authenticate_ignores_last_used_save_failure \
  test.test_account_image_capabilities.AuthServiceTests.test_update_user_key_replaces_raw_key
```

Expected: all listed tests PASS。

- [ ] **Step 6: 提交认证服务实现**

```bash
git add services/auth_service.py test/test_account_image_capabilities.py
git commit -m "feat: add user key web login permission"
```

## Task 3: 写 API 层失败测试

**Files:**
- Modify: `test/test_quota_api.py:16-43,139-170`
- Modify: `test/test_system_api.py:48-70`

- [ ] **Step 1: 扩充用户密钥 API fake 的字段和更新范围**

在 `FakeAuthService.create_key()` 的 `item` 中加入：

```python
            "web_login_enabled": True,
```

将 `FakeAuthService.update_key()` 的更新白名单改为：

```python
            item.update({key: value for key, value in updates.items() if key in {"name", "enabled", "web_login_enabled"}})
```

- [ ] **Step 2: 添加 API 更新字段测试**

在 `QuotaApiTests` 中加入：

```python
    def test_update_user_key_changes_web_login_permission(self):
        created = self.client.post(
            "/api/auth/users",
            headers=AUTH_HEADERS,
            json={"name": "Web 登录测试"},
        ).json()["item"]
        self.assertTrue(created["web_login_enabled"])

        response = self.client.post(
            f"/api/auth/users/{created['id']}",
            headers=AUTH_HEADERS,
            json={"web_login_enabled": False},
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["item"]["web_login_enabled"])
        self.assertFalse(response.json()["items"][0]["web_login_enabled"])
```

- [ ] **Step 3: 添加 `/auth/login` 使用 Web 专用身份校验的测试**

在 `SystemLogApiTests` 中加入：

```python
    def test_login_uses_web_identity_check(self) -> None:
        identity = {"id": "user-1", "name": "普通用户", "role": "user"}
        with (
            mock.patch.object(system_module, "require_web_identity", return_value=identity) as web_check,
            mock.patch.object(system_module, "require_identity", side_effect=AssertionError("login used API identity check")),
        ):
            response = self.client.post("/auth/login", headers={"Authorization": "Bearer user-key"})

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["role"], "user")
        web_check.assert_called_once_with("Bearer user-key")
```

- [ ] **Step 4: 运行 API 测试确认当前路由实现失败**

```bash
python3 -m unittest \
  test.test_quota_api.QuotaApiTests.test_update_user_key_changes_web_login_permission \
  test.test_system_api.SystemLogApiTests.test_login_uses_web_identity_check
```

Expected: FAIL，分别表现为更新请求未被识别为有效修改，以及系统模块尚未提供/使用 `require_web_identity`。

- [ ] **Step 5: 提交 API 失败测试**

```bash
git add test/test_quota_api.py test/test_system_api.py
git commit -m "test: cover web login API permission flow"
```

## Task 4: 实现 API 鉴权和用户密钥更新

**Files:**
- Modify: `api/support.py:20-45`
- Modify: `api/system.py:11,66-75`
- Modify: `api/accounts.py:47-52,215-223`

- [ ] **Step 1: 在 support 层复用身份提取逻辑并增加 Web 校验入口**

将现有身份校验整理为：

```python
def _require_identity(authorization: str | None, *, web_login: bool = False) -> dict[str, object]:
    token = extract_bearer_token(authorization)
    if web_login:
        identity = _legacy_admin_identity(token) or auth_service.authenticate_web_login(token)
    else:
        identity = _legacy_admin_identity(token) or auth_service.authenticate(token)
    if identity is None:
        raise HTTPException(status_code=401, detail={"error": "密钥无效或已失效，请重新登录"})
    return identity


def require_identity(authorization: str | None) -> dict[str, object]:
    return _require_identity(authorization)


def require_web_identity(authorization: str | None) -> dict[str, object]:
    return _require_identity(authorization, web_login=True)
```

保持 `require_auth_key()` 和 `require_admin()` 继续调用 `require_identity()`。

- [ ] **Step 2: 切换系统登录路由**

在 `api/system.py` 导入 `require_web_identity`，并将登录函数中的身份校验改为：

```python
        identity = require_web_identity(authorization)
```

- [ ] **Step 3: 扩充更新请求模型和更新字段映射**

在 `UserKeyUpdateRequest` 中加入：

```python
    web_login_enabled: bool | None = None
```

在 `update_user_key()` 的更新字典中加入：

```python
                "web_login_enabled": body.web_login_enabled,
```

- [ ] **Step 4: 运行 API 测试确认通过**

```bash
python3 -m unittest \
  test.test_quota_api.QuotaApiTests.test_create_user_key_accepts_initial_limits \
  test.test_quota_api.QuotaApiTests.test_update_user_key_changes_web_login_permission \
  test.test_quota_api.QuotaApiTests.test_list_and_update_return_remaining_and_reject_low_limit \
  test.test_system_api.SystemLogApiTests.test_login_uses_web_identity_check \
  test.test_system_api.SystemLogApiTests.test_get_logs_uses_server_pagination_and_threadpool \
  test.test_system_api.SystemLogApiTests.test_scheduler_metrics_are_admin_protected_and_redacted
```

Expected: all listed tests PASS。

- [ ] **Step 5: 提交 API 实现**

```bash
git add api/support.py api/system.py api/accounts.py test/test_quota_api.py test/test_system_api.py
git commit -m "feat: enforce user key web login permission"
```

## Task 5: 实现设置页的可见性和编辑入口

**Files:**
- Modify: `web/src/lib/api.ts:321-336,711-724`
- Modify: `web/src/app/settings/components/user-keys-card.tsx:4,83-90,219-261,320-340,502-572`

- [ ] **Step 1: 扩充前端用户密钥类型和更新请求**

在 `UserKey` 的 `enabled` 后加入：

```typescript
  web_login_enabled: boolean;
```

在 `updateUserKey()` 的 `updates` 类型中加入：

```typescript
    web_login_enabled?: boolean;
```

- [ ] **Step 2: 添加编辑状态和保存逻辑**

导入 `Checkbox`：

```typescript
import { Checkbox } from "@/components/ui/checkbox";
```

增加状态：

```typescript
  const [editWebLoginEnabled, setEditWebLoginEnabled] = useState(true);
```

在 `openEditDialog()` 中加入：

```typescript
    setEditWebLoginEnabled(item.web_login_enabled);
```

在 `handleEdit()` 的 `limitsChanged` 后加入比较和空更新判断：

```typescript
    const webLoginChanged = editWebLoginEnabled !== item.web_login_enabled;
    if (trimmedName === item.name && !trimmedKey && !limitsChanged && !webLoginChanged) {
```

在 `updateUserKey()` 请求对象中加入：

```typescript
        ...(webLoginChanged ? { web_login_enabled: editWebLoginEnabled } : {}),
```

并把成功提示计算为：

```typescript
      toast.success(
        webLoginChanged
          ? "用户密钥与 Web 登录权限已更新"
          : limitsChanged
            ? "用户密钥与额度已更新"
            : trimmedKey
              ? "用户密钥已更新"
              : "用户名称已更新",
      );
```

- [ ] **Step 3: 在列表展示独立状态**

在全局启用 Badge 后加入：

```tsx
                        <Badge variant={item.web_login_enabled ? "info" : "secondary"} className="rounded-md">
                          {item.web_login_enabled ? "允许 Web 登录" : "禁止 Web 登录"}
                        </Badge>
```

同时把卡片说明改为：

```tsx
                <p className="text-sm text-stone-500">为普通用户创建专用密钥；可分别控制 API 使用状态和 Web 登录权限。</p>
```

- [ ] **Step 4: 在编辑对话框加入二态 Checkbox**

在额度输入网格之后、`DialogFooter` 之前加入：

```tsx
            <div className="flex items-start gap-3 rounded-xl border border-stone-200 bg-stone-50 px-4 py-3">
              <Checkbox
                id="edit-web-login-enabled"
                checked={editWebLoginEnabled}
                onCheckedChange={(checked) => setEditWebLoginEnabled(checked === true)}
                disabled={editingItem ? pendingIds.has(editingItem.id) : false}
                className="mt-0.5"
              />
              <div className="space-y-1">
                <label htmlFor="edit-web-login-enabled" className="text-sm font-medium text-stone-700">
                  允许 Web 登录
                </label>
                <p className="text-xs leading-5 text-stone-500">
                  关闭后该密钥仍可调用 API，但不能用于登录 Web。
                </p>
              </div>
            </div>
```

关闭编辑对话框时保留现有清理逻辑，下一次打开必须由 `openEditDialog()` 从对应的密钥重新设置开关状态。

- [ ] **Step 5: 运行前端类型检查、lint 和生产构建**

```bash
cd web
npx tsc --noEmit
npx eslint src/lib/api.ts src/app/settings/components/user-keys-card.tsx
npm run build
```

Expected: TypeScript、ESLint 和 Next.js production build 均以退出码 0 完成。

- [ ] **Step 6: 提交前端实现**

```bash
cd ..
git add web/src/lib/api.ts web/src/app/settings/components/user-keys-card.tsx
git commit -m "feat: add web login toggle for user keys"
```

## Task 6: 全量验证、inline review、提交与部署

**Files:**
- Verify: all changed files and current Docker deployment.

- [ ] **Step 1: 对照设计规格和计划检查变更范围**

```bash
git status --short
git diff HEAD~4..HEAD --check
git diff HEAD~4..HEAD -- services/auth_service.py api/support.py api/system.py api/accounts.py web/src/lib/api.ts web/src/app/settings/components/user-keys-card.tsx
```

检查以下不变量：

- `enabled=false` 仍同时阻止 API 和 Web。
- `web_login_enabled=false` 只在 `authenticate_web_login()` 生效。
- `require_identity()`、`require_admin()` 和 API 业务路由没有切换到 Web 专用认证。
- 兼容配置中的 legacy admin key 和 `role=admin` 密钥仍可登录。
- 缺字段历史记录归一化为允许 Web 登录。
- 列表状态、编辑对话框状态和保存请求使用同一个字段名。
- 没有输出密钥原文、无关日志或修改无关配置。

- [ ] **Step 2: 运行后端全量测试**

```bash
python3 -m unittest discover -s test -p 'test_*.py'
```

Expected: 测试进程退出码为 0，失败数为 0。

- [ ] **Step 3: 运行最终前端验证**

```bash
cd web
npx tsc --noEmit
npx eslint src/lib/api.ts src/app/settings/components/user-keys-card.tsx
npm run build
cd ..
```

Expected: 三个命令均退出码为 0。

- [ ] **Step 4: 做一次 inline 严格 review**

逐文件检查 `git diff`，重点确认：

- Web 登录入口使用 `require_web_identity`，API 入口仍使用 `require_identity`。
- 用户密钥旧数据、新建、更新、列表和认证返回值都覆盖默认值。
- 管理员字段不会被错误编辑或被用户开关拒绝。
- API 更新只提交发生变化的字段，额度更新和密钥替换行为没有回归。
- 前端关闭 Web 登录后仍保留 API 说明，视觉状态不只依赖颜色。
- 发现问题时先修复，再重复受影响的测试、类型检查和构建；没有未处理的 Critical/Important 问题后才继续。

- [ ] **Step 5: 提交最终集成提交**

```bash
git add services/auth_service.py api/support.py api/system.py api/accounts.py web/src/lib/api.ts web/src/app/settings/components/user-keys-card.tsx test/test_account_image_capabilities.py test/test_quota_api.py test/test_system_api.py
git commit -m "feat: control user key web login access"
```

如果前面各任务已经提交且工作树为空，这一步只需确认没有遗漏，不创建空提交。

- [ ] **Step 6: 读取当前容器配置并构建新镜像**

在停止服务前读取旧容器的镜像、环境、挂载、端口和重启策略，部署时沿用这些运行参数：

```bash
docker inspect my-chatgpt2api > /tmp/my-chatgpt2api.inspect.json
docker ps --filter name=^/my-chatgpt2api$ --format '{{.Names}} {{.Image}} {{.Ports}}'
```

构建带当前提交标识的镜像：

```bash
IMAGE_TAG="my-chatgpt2api:user-key-web-login-$(git rev-parse --short HEAD)"
docker build -t "$IMAGE_TAG" .
```

- [ ] **Step 7: 可回滚地替换 `localhost:3000` 服务**

先停止并重命名旧容器，保留它作为回滚副本；再使用 inspect 中确认过的原有环境、挂载、端口和重启参数启动新容器，容器名保持 `my-chatgpt2api`：

```bash
DEPLOY_SUFFIX="$(date +%Y%m%d%H%M%S)"
docker stop my-chatgpt2api
docker rename my-chatgpt2api "my-chatgpt2api-old-${DEPLOY_SUFFIX}"
```

`docker run` 命令必须根据 `/tmp/my-chatgpt2api.inspect.json` 中的真实配置生成，不得遗漏持久化数据卷或环境变量；端口必须保持 `-p 3000:80`。启动后确认：

```bash
docker ps --filter name=^/my-chatgpt2api$ --format '{{.Names}} {{.Image}} {{.Ports}}'
curl -fsS http://127.0.0.1:3000/version
curl -fsS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3000/
docker logs --tail 80 my-chatgpt2api
```

如果新容器启动失败，停止新容器并把旧容器重命名回 `my-chatgpt2api`、重新启动，再报告失败日志；不得删除旧回滚副本。

- [ ] **Step 8: 做上线后的权限烟测**

使用管理员登录 Web 设置页创建或选择一个测试用户密钥，验证：

1. 新建密钥列表显示“允许 Web 登录”。
2. 关闭开关并保存，列表显示“禁止 Web 登录”。
3. 该用户密钥请求 `/auth/login` 返回 401。
4. 同一密钥在 `enabled=true` 时调用一个 API，鉴权仍成功。
5. 重新打开开关后 `/auth/login` 恢复成功。
6. 管理员密钥登录和管理接口不受影响。

## 完成判定

- 后端全量 unittest、前端 TypeScript、ESLint、生产构建全部通过。
- inline review 没有 Critical 或 Important 问题。
- 当前 `localhost:3000` 返回新版本健康响应，容器使用新镜像运行。
- 旧容器仍保留为回滚副本，工作树状态和最终提交已记录。
