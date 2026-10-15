# 用户密钥额度系统设计

- 日期：2026-09-08
- 状态：已确认，待实现
- 目标分支：`feat/high-performance-scheduler`

## 1. 背景与目标

当前用户密钥只负责身份认证，没有独立的文本或图片额度。管理员密钥和普通用户密钥需要区分：管理员不受额度限制，普通用户必须受文本额度和图片额度约束。

本设计覆盖：

- 用户密钥的文本额度；
- 图片生成和图片编辑共用的图片额度；
- 成功请求后的原子结算；
- 图片多输出和部分成功场景；
- SQLite 持久化、并发安全和崩溃恢复；
- Web 端额度编辑和实时查看。

## 2. 已确认规则

### 2.1 身份与额度

- `admin` 角色以及旧版管理员密钥不受额度限制；
- 普通用户密钥拥有独立的文本额度和图片额度；
- 新建普通用户密钥时，文本额度和图片额度默认都是 `0`；
- 额度是永久总额度，不自动按日、周或月重置；
- 管理员可以在 Web 端调整总额度；
- 图片额度由图片生成和图片编辑共享。

### 2.2 计费单位

- 一次完整成功的文本请求扣减 `1`；
- 图片生成按成功输出图片数扣减；
- 图片编辑按成功输出图片数扣减；
- 图片接口 `n=4` 且全部成功时扣减 `4`；
- `n=4` 但只成功返回 3 张时扣减 `3`；
- 完全失败的请求扣减 `0`；
- 输入校验失败、内容过滤失败、调度器拒绝、上游失败、超时和取消均不扣减已用额度。

### 2.3 预留与结算

额度不能在请求结束后才直接扣减，否则并发请求会同时看到相同余额并造成超扣。因此采用预留、结算、释放模型：

```text
校验请求
  -> 普通用户原子预留额度
  -> 调用 scheduler / 上游
  -> 成功：提交实际消耗数量
  -> 失败：释放预留额度
```

图片请求先预留请求的 `n` 张额度。部分成功时提交实际输出数量，剩余预留释放。额度不足时，在调用上游前返回 `429 insufficient_quota`。

## 3. 存储设计

新增独立 SQLite 数据库：

```text
data/auth_quota.db
```

额度数据库不依赖当前账号存储后端。即使账号和用户密钥仍使用 JSON 存储，额度仍通过 SQLite 的事务保证并发安全。数据库启用：

- WAL journal mode；
- `synchronous=NORMAL`；
- `busy_timeout`；
- 以 `key_id` 和 `reservation_id` 建立索引。

### 3.1 额度汇总表

```text
key_quotas
----------
key_id              TEXT PRIMARY KEY
text_limit          INTEGER NOT NULL DEFAULT 0
text_used           INTEGER NOT NULL DEFAULT 0
text_reserved       INTEGER NOT NULL DEFAULT 0
image_limit         INTEGER NOT NULL DEFAULT 0
image_used          INTEGER NOT NULL DEFAULT 0
image_reserved      INTEGER NOT NULL DEFAULT 0
created_at          REAL NOT NULL
updated_at          REAL NOT NULL
deleted_at          REAL NULL
```

约束：

- 所有额度字段必须是非负整数；
- `used` 和 `reserved` 不允许由 Web 端直接编辑；
- 管理员调整总额度时，新额度不能低于 `used + reserved`；
- `remaining` 不单独持久化，始终计算为 `limit - used - reserved`。

### 3.2 预留表

```text
quota_reservations
------------------
reservation_id      TEXT PRIMARY KEY
key_id              TEXT NOT NULL
quota_type          TEXT NOT NULL
requested_units     INTEGER NOT NULL
committed_units     INTEGER NOT NULL DEFAULT 0
status              TEXT NOT NULL
request_id          TEXT NULL
endpoint            TEXT NOT NULL DEFAULT ''
created_at          REAL NOT NULL
expires_at          REAL NOT NULL
settled_at          REAL NULL
```

`quota_type` 取值为 `text` 或 `image`；`status` 取值为 `reserved`、`committed`、`released`、`expired`。

预留表用于：

- 防止重复结算；
- 支持图片部分成功；
- 支持进程崩溃后的超时回收；
- 记录最小必要的额度审计信息；
- 让图片任务恢复轮询时复用原预留记录。

### 3.3 原子预留

预留操作使用 `BEGIN IMMEDIATE`：

1. 锁定对应 `key_quotas` 行；
2. 计算 `limit - used - reserved`；
3. 余额不足则回滚并返回额度不足；
4. 增加对应 `reserved`；
5. 写入 `quota_reservations`；
6. 提交事务。

结算和释放同样使用事务，并通过 `WHERE status = 'reserved'` 保证幂等。重复结算不会重复增加 `used`。

### 3.4 崩溃恢复

每条预留记录都有 `expires_at`。后台 watchdog 定期扫描过期的 `reserved` 记录，在事务中：

- 将汇总表中的 `reserved` 减回去；
- 将记录标记为 `expired`；
- 保留结算记录用于排查。

图片任务的过期时间应覆盖其允许的上游执行时间和恢复轮询时间，并额外保留清理宽限期。长时间运行的任务在仍处于活动状态时必须续租预留；watchdog 只回收已确认终止、已取消或已失联的请求，不能在正常执行期间提前释放额度。

预留一旦进入 `expired` 或其他终态，晚到的上游结果不得直接作为成功响应返回，避免出现“额度已释放但用户拿到成功结果”。调用方应将该请求标记为失败并记录原因；图片任务则标记为恢复失败，必要时丢弃晚到输出。

## 4. 服务层设计

新增 `QuotaService`，对 API 层和图片任务服务提供统一接口：

```python
reserve(identity, quota_type, units, request_id, endpoint, expires_at)
commit(reservation_id, committed_units)
release(reservation_id)
get_quota(key_id)
set_limits(key_id, text_limit, image_limit)
```

服务层规则：

- `identity.role == "admin"` 时返回无操作的管理员预留句柄；
- 普通用户只使用 `identity.id` 定位额度，不使用明文密钥；
- 额度记录不存在的普通用户密钥按 `0` 额度处理；
- 额度不足抛出可映射为 `429` 的领域异常；
- 结算、释放和过期回收都必须幂等。

## 5. 请求接入范围

### 5.1 文本额度

以下请求计入文本额度：

- `/v1/chat/completions`；
- `/v1/responses`；
- `/v1/messages`；
- `/v1/search`。

文本请求在输入校验和内容过滤成功后、进入 scheduler 前预留 `1`。非流式响应正常返回时提交 `1`；流式响应只有正常结束才提交，异常中断则释放。

缓存命中和并发去重返回成功响应时仍计为一次用户请求，避免通过缓存绕过额度限制。

### 5.2 图片额度

以下请求计入图片额度：

- `/v1/images/generations`；
- `/v1/images/edits`；
- `/api/image-tasks/generations`；
- `/api/image-tasks/edits`。

直接图片接口从 `n` 读取请求数量，并按 `1-4` 校验。图片任务接口当前固定产生 1 张；以后支持多输出时复用相同的 `n` 规则。

同步图片请求在完整响应后按返回数据项数量结算。图片任务在创建时预留，后台任务成功时结算，失败、取消、超时或恢复失败时释放。任务恢复轮询只引用原 `reservation_id`，不创建新预留。

### 5.3 不计费接口

以下接口不扣额度：

- 模型列表；
- 用户密钥和系统管理接口；
- 图片任务查询和恢复轮询；
- 图片下载和图片管理；
- 账号管理和健康检查。

## 6. 错误语义

额度不足统一返回 HTTP `429`：

```json
{
  "error": {
    "message": "text quota exhausted",
    "type": "insufficient_quota",
    "code": "insufficient_quota"
  }
}
```

由于额度是永久总额度，额度不足响应不返回误导性的短 `Retry-After`。调度队列满、上游限流和用户额度不足仍通过不同错误消息区分。

## 7. 管理 API

### 7.1 创建用户密钥

扩展 `POST /api/auth/users`：

```json
{
  "name": "设计组",
  "text_limit": 1000,
  "image_limit": 100
}
```

两个额度字段缺省为 `0`。创建响应继续只返回一次明文密钥，同时返回额度摘要。

### 7.2 查询用户密钥

扩展 `GET /api/auth/users` 返回：

```json
{
  "id": "key-id",
  "name": "设计组",
  "role": "user",
  "enabled": true,
  "text_limit": 1000,
  "text_used": 120,
  "text_reserved": 2,
  "text_remaining": 878,
  "image_limit": 100,
  "image_used": 35,
  "image_reserved": 4,
  "image_remaining": 61,
  "created_at": "...",
  "last_used_at": "..."
}
```

该接口仍然只允许管理员访问。

### 7.3 修改额度

扩展 `POST /api/auth/users/{key_id}`，支持：

```json
{
  "text_limit": 2000,
  "image_limit": 300
}
```

字段表示永久总额度，而不是剩余额度。修改在 SQLite 事务中完成；如果新值低于 `used + reserved`，返回 `409 quota_limit_below_usage`，不产生部分更新。

本期不直接修改 `used`。管理员通过调整总额度增加或减少剩余额度，历史消耗保持可审计。

## 8. Web 管理界面

在现有用户密钥管理卡片中增加：

- 创建时填写文本总额度和图片总额度，默认 `0`；
- 列表显示总额度、已用、执行中预留、剩余；
- 编辑弹窗修改名称、密钥、启用状态和两类总额度；
- 非负整数校验；
- 服务端返回额度下限冲突时展示明确错误；
- 保存后立即刷新列表；
- 页面可见时每 5 秒轮询一次，页面隐藏时暂停；
- 不引入 WebSocket，避免为少量管理员额度数据增加长连接开销。

前端 API 类型应同步扩展 `UserKey`、创建请求和更新请求。管理员密钥不作为普通用户密钥展示，也不显示额度限制。

## 9. 迁移、备份与删除

### 9.1 现有数据迁移

- 启动时初始化 `auth_quota.db` 表；
- 读取现有用户密钥并以 `INSERT ... ON CONFLICT DO NOTHING` 补建缺失额度行；
- 新补建的普通用户额度为 `0`；
- 管理员密钥不创建额度行；
- 已删除或已存在的额度行不被启动迁移覆盖；
- 缺失额度行按 `0` 处理，保证失败安全。

### 9.2 备份

备份内容增加 `auth_quota.db`。恢复时先恢复额度数据库，再加载用户密钥和任务状态。额度数据库和任务状态必须同时备份，避免恢复后出现预留无法结算的问题。

### 9.3 禁用和删除

- 禁用或删除用户密钥后禁止新请求；
- 已经预留的任务允许继续完成；
- 额度行在所有预留结束后再清理；
- 历史结算记录保留到既有数据保留周期结束。

## 10. 测试验收

### 10.1 额度服务单测

- 管理员预留始终放行且不写入用户额度；
- 普通用户默认额度为 `0`；
- 预留、提交、释放的余额计算正确；
- 文本成功扣 1，失败释放；
- 图片 `n=4` 全成功扣 4；
- 图片部分成功按实际输出数量扣减；
- 完全失败不扣；
- 重复提交和重复释放不会重复改变余额；
- 过期预留可回收；
- 额度修改不能低于已用量和预留量。

### 10.2 并发与 SQLite 测试

- 多线程同时预留时不会超过可用额度；
- 多个进程连接同一个 SQLite 数据库时结果一致；
- WAL 和 busy timeout 下无未处理的锁冲突；
- 崩溃后 watchdog 能释放悬挂预留。

### 10.3 API 集成测试

- 文本、图片生成、图片编辑接口均传递正确额度类型；
- 图片任务创建、完成、失败和恢复不重复扣减；
- 管理员、普通用户、禁用密钥的权限边界正确；
- 额度不足时不会调用上游；
- 缓存命中仍按请求计费。

### 10.4 Web 验收

- 新建密钥可填写两类额度；
- 列表实时显示 used/reserved/remaining；
- 修改总额度后立即刷新；
- 并发请求期间预留额度实时反映；
- 非法额度和低于已用量的额度能正确提示。

## 11. 实现顺序

1. 新增 SQLite `AuthQuotaStore` 和初始化迁移；
2. 新增 `QuotaService` 的预留、提交、释放、过期回收；
3. 接入用户密钥创建、查询、更新和删除；
4. 接入文本同步接口；
5. 接入图片生成、图片编辑和图片任务；
6. 扩展 Web 用户密钥管理；
7. 添加单测、并发测试和 API 集成测试；
8. 更新备份内容；
9. 构建镜像、部署并验证额度扣减和管理员豁免。

## 12. 非目标与风险

- 本期不实现按日/月自动重置；
- 本期不按 token 数计费；
- 本期不把上游账号 quota 与用户密钥额度合并；
- 本期不引入 WebSocket；
- SQLite 适合当前单实例/少量实例场景，多实例跨主机部署时需要共享存储或集中式额度服务；
- 旧版本如果绕过新额度数据库运行，可能无法执行用户额度限制，部署回滚时需要同时回滚认证和额度相关代码。
