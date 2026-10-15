# 用户密钥请求并发限制设计

## 目标

为每个普通用户密钥增加独立的文本请求和图片请求并发上限，并通过 Web 管理页面配置。达到上限的新请求立即返回 `429`；管理员密钥不受限制；配置保存后对下一次请求立即生效。

## 已确认的行为

- 限制按用户密钥 ID 独立计算，不同用户密钥互不影响。
- 文本和图片使用两个独立计数器。
- 默认文本并发为 `4`，默认图片并发为 `1`。
- 限制对象为 `role=user`；管理员密钥和兼容管理员身份免限流。
- 达到限制时不排队，返回 `429` 和 `Retry-After: 1`。
- `/v1/images/generations`、`/v1/images/edits`、图片 Chat/Responses 请求，以及 `/api/image-tasks/generations` 和 `/api/image-tasks/edits` 都计入图片并发。
- 异步图片任务从创建成功开始占用名额，直到成功或失败终态释放；创建失败、调度失败和异常回滚都释放。
- 相同用户密钥重复提交同一 `client_task_id` 时返回已有任务，不重复占用名额。
- 任务超时后执行 `resume-poll` 时按一次新的图片操作获取名额；该操作结束后释放，避免旧 lease 泄漏或重复占用。
- 这是进程内限制，不将短期在途计数写入 SQLite。它与现有上游账号 scheduler 的并发限制保持独立。

## 架构

新增 `services/user_key_concurrency.py`，提供进程内 `UserKeyConcurrencyLimiter` 和幂等 lease。限流器使用线程锁保护 `(key_id, request_type)` 到活动数的映射，每次获取都通过 `QuotaService` 读取该用户密钥在 SQLite 中的当前并发字段，因此不需要重启或重建 scheduler。调大限制立即允许新请求，调小限制不会中断已在途请求，只会拒绝直到活动数降到新限制以下。

限流器的核心接口为：

```python
lease = limiter.try_acquire(identity, "text")
if lease is None:
    # reject with 429
lease.release()  # safe to call more than once
```

管理员身份返回可释放的 no-op lease。普通用户身份使用 `identity["id"]` 作为计数键；身份缺失或类型不在 API 入口允许范围内时不改变现有认证行为。

普通 `/v1` 请求统一通过 `api.support.run_with_quota` 获取 lease。获取动作在配额预留之前执行，避免超限请求消耗配额。lease 的释放与现有配额结算回调绑定：普通响应在 `LoggedCall.run` 返回结果后释放，流式响应在正常结束、异常结束或客户端取消时释放；预留失败、handler 异常和回调异常都通过幂等释放兜底。

异步图片任务由 `ImageTaskService` 持有以任务 key 为索引的 lease 表。新任务在去重检查后、配额预留前获取图片 lease；配额预留、任务持久化或调度失败时回滚。任务执行完成、失败、取消或调度回调报告异常时释放。服务重启恢复任务时按任务 owner 重新建立运行时 lease，不能把恢复任务的内部 system identity 当作管理员而绕过用户限制。

## 配置与 Web

在现有 SQLite `key_quotas` 表中增加两个按密钥保存的字段：

```sql
ALTER TABLE key_quotas
  ADD COLUMN text_concurrency INTEGER NOT NULL DEFAULT 4 CHECK (text_concurrency > 0);
ALTER TABLE key_quotas
  ADD COLUMN image_concurrency INTEGER NOT NULL DEFAULT 1 CHECK (image_concurrency > 0);
```

已有数据库启动时自动补列，旧密钥使用兼容默认值 `4/1` 初始化；新建密钥也写入自己的初始值。并发修改通过用户密钥 API 与额度更新在同一条保存链路中完成。

`GET /api/auth/users` 返回每条密钥的 `text_concurrency` 和 `image_concurrency`；创建与更新接口接受同名字段，要求正整数。Web 的“用户密钥”页在密钥列表中显示两个上限，并在创建/编辑弹窗中允许逐条查看和修改。保存成功后列表使用服务端返回值更新；限流器下一次准入直接读取新值，立即生效。管理员密钥仍然不创建/读取限流计数。旧的顶层 `config` 并发字段只作为迁移和兼容默认值，不作为用户密钥页的全局控制入口。

## 错误协议

并发超限使用统一错误码 `user_key_concurrency_limit`，类型为 `rate_limit_error`，消息区分 `text` 或 `image`。`/v1/*` 通过现有 OpenAI/Anthropic 错误转换器返回兼容格式；`/api/image-tasks/*` 使用现有任务 API 的错误包装。两类响应都返回状态码 `429` 和响应头 `Retry-After: 1`。

## 测试策略

后端单元测试验证：

- 文本/图片计数隔离、用户密钥隔离和管理员放行。
- SQLite 旧表迁移、每条密钥默认值和并发字段独立更新。
- 达到上限时拒绝，释放后再次允许，lease 重复释放不会造成负计数。
- 运行时修改单条密钥的限制立即影响下一次获取，缩小限制不影响现有 lease。
- 普通同步请求和流式请求在成功、异常、取消时都释放。
- 图片异步任务的去重、终态释放、失败回滚和限流错误转换。
- 用户密钥 API 的创建、列表和更新字段，以及并发值校验。

完成后运行 Python 全量测试，并在 `web` 目录执行现有可用的 TypeScript/build 校验。
