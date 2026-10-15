# SQLite 短生命周期线程连接泄漏修复

## 状态

已确认设计，类型：bug fix。

实施分支：`feat/memory-optimization-sqlite`

实施基线：`8de4508` (`feat: configure per-key user concurrency`)

本 spec 只针对上述分支和最新提交。其他分支的内存行为不在本次范围内。

## 事件摘要

`dedi` 上的服务运行约十几个小时后，Python 进程 RSS 达到约 450-640 MiB，匿名内存约 450-550 MiB，swap 使用超过 500 MiB；用户观测到进程约占用 700 MiB、swap 约 400 MiB。进程线程数约 29，但 `/proc/<pid>/fd` 中 `app.db` 和 `app.db-wal` 的描述符数量分别达到 2160。

线上配置为：

- 账号数量：99；
- 账号刷新周期：5 分钟；
- `refresh_accounts()` 每轮最多创建 10 个 worker；
- 状态数据库：`/app/data/app.db`。

运行约 18 小时对应约 216 个刷新周期，`216 * 10 = 2160`，与线上观察到的 SQLite 连接/数据库描述符数量一致。每个 SQLite 连接在 WAL 模式下会对应 `app.db` 和 `app.db-wal` 文件描述符，因此当前数量是连接数累积的直接证据。

容器内的 `app.db` 约 1.6 MiB，日志库约 205 MiB；图片目录、缩略图目录和图片任务 spool 为空。图片请求路径在请求结束时释放输入列表和临时文件，日志路径也会剥离 Base64/图片正文。图片数据可能造成单请求峰值，但没有证据表明它解释了本事件中随刷新周期线性增加的 SQLite FD、匿名内存和 swap。

## 根因

### 直接原因

`SQLiteStateStore`、`SchedulerStateStore` 和 `AuthQuotaStore` 都使用相同的连接缓存模式：

1. 通过 `threading.local()` 为每个线程创建一个 SQLite 连接；
2. 将连接加入实例级 `_connections` 集合，以便应用退出时统一关闭；
3. 只在 store 的全量 `close()` 中关闭这些连接；
4. 没有为所有 worker 线程提供统一的 `close_current_thread()` 生命周期出口。

`AccountService.refresh_accounts()` 每 5 分钟新建一个 `ThreadPoolExecutor`，最多创建 10 个 worker。每个 worker 调用 `fetch_remote_info()` 并最终更新账号，首次访问 SQLite store 时创建连接。executor 等待任务结束后退出，但连接仍被 store 的 `_connections` 强引用；下一个刷新周期继续创建新线程和新连接，旧连接不会释放。

因此连接数量近似按以下公式增长：

```text
连接数 ~= 刷新周期数 * refresh worker 数
```

SQLite page cache、Python 对象、连接内部状态和 WAL 文件描述符共同形成匿名内存及 swap 压力。当前现象符合资源生命周期泄漏，不是单纯的 Python heap 碎片，也不是图片长期驻留。

### 为什么现有 `close()` 没有解决问题

`close()` 只在应用整体关闭时调用。它没有在 `ThreadPoolExecutor` worker 任务完成、异常、取消或 executor 退出时执行，因此无法处理每轮刷新产生的短生命周期连接。scheduler writer 已经有局部的 `close_current_thread()` 调用，但三个 SQLite store 的连接生命周期实现不统一，未来新增线程池仍容易复现同类问题。

## 修复目标

1. 连接生命周期必须与拥有它的线程生命周期一致，短生命周期 worker 退出后不保留连接。
2. 连接数量必须由当前存活的 SQLite 使用线程数决定，不能由历史 executor 创建次数决定。
3. 统一三个 SQLite store 的连接创建、登记、当前线程释放、死线程回收和应用关闭行为。
4. 保留现有 WAL、`busy_timeout`、`cache_size`、事务和业务并发语义。
5. 账号刷新异常、取消和正常结束都必须释放当前 worker 的连接。
6. 不改变图片请求、图片编辑、图片存储禁用和日志脱敏逻辑。

## 非目标

- 不在本次 bug fix 中把 SQLite 改为 PostgreSQL、Redis 或其他外部数据库；
- 不通过降低文本、图片、编辑或 scheduler 并发上限来掩盖内存问题；
- 不把 `app.db` 改成单一全局 SQLite 连接；
- 不重写 SQLite schema、迁移流程或业务事务边界；
- 不以清空日志、删除任务历史或关闭 swap 作为修复手段；
- 不对图片 bytes 做与本问题无关的行为变更。

## 推荐设计

### 1. 新增统一连接生命周期组件

新增 `services/storage/sqlite_connection.py`，提供一个小型 `SQLiteConnectionRegistry`，由三个 SQLite store 使用。该组件只负责连接生命周期，不负责 SQL、schema 或业务事务。

建议接口：

```python
class SQLiteConnectionRegistry:
    def connection(self) -> sqlite3.Connection: ...
    def close_current_thread(self) -> None: ...
    def reap_dead_threads(self) -> int: ...
    def close(self) -> None: ...
    def connection_count(self) -> int: ...
```

连接配置由调用方提供或由 registry 接受配置回调，以保留三个 store 当前的差异：

- `SQLiteStateStore` 和 `SchedulerStateStore` 的事务模式；
- `AuthQuotaStore` 的 `isolation_level=None` 和显式 `BEGIN IMMEDIATE`；
- 各 store 当前的 `row_factory`、WAL、busy timeout、cache size、foreign key 和 quota WAL checkpoint 设置。

registry 必须满足以下不变量：

- 每个 registry 在一个线程上最多拥有一个连接；
- 连接创建后登记 owner `Thread` 的弱引用或等效可验证身份，不能只保存可复用的裸 thread ident；
- `close_current_thread()` 幂等，只关闭调用线程持有的连接，并从 registry 中移除；
- `close()` 关闭所有已登记连接，调用前必须已经停止会使用该 registry 的后台线程；
- `connection()` 在创建新连接前回收已退出线程的连接；
- `reap_dead_threads()` 遇到已经结束的 owner 时关闭连接并移除登记，即使调用方漏掉了 `finally` 也不会永久积累；
- 连接关闭失败只记录可诊断日志，不阻止其他连接继续清理；
- registry 不保存业务对象、查询结果或图片 bytes。

这里的“有界”指连接数由当前存活线程数和正在使用 registry 的长期 executor 数量决定；它不再随历史刷新次数单调增长。长期存在的 scheduler worker 可以保留一个连接，但必须在 worker/executor 停止后被回收。

### 2. 改造三个 SQLite store

涉及：

- `services/storage/sqlite_state.py`；
- `services/scheduler/state_store.py`；
- `services/quota_service.py`。

改造要求：

1. 移除各自重复的 `_local`、`_connections`、连接创建和全量关闭实现，改由 registry 承担。
2. 保留现有 `_connection()` 方法作为 store 内部兼容入口，内部转发到 registry，避免一次性改动所有 SQL 调用点。
3. 三个 store 暴露 `close_current_thread()`，转发到 registry；非 SQLite backend 不需要实现该方法，调用方继续使用可选能力检测。
4. 三个 store 的 `close()` 继续存在，并在服务停止顺序正确的前提下关闭全部连接。
5. 所有现有事务、`_write_lock`、初始化锁、异常 rollback/commit 行为保持不变。
6. 为测试提供只读的 `connection_count()` 或等效诊断入口；生产日志不打印 token、请求正文或图片内容。

### 3. 修复账号刷新 worker 生命周期

涉及：`services/account_service.py`。

在 `refresh_accounts()` 提交给 executor 的 worker 外包一层生命周期 wrapper：

```python
try:
    return self.fetch_remote_info(...)
finally:
    close_current_thread = getattr(self.storage, "close_current_thread", None)
    if callable(close_current_thread):
        close_current_thread()
```

要求：

- 正常返回、业务异常、网络异常、`CancelledError`、executor 取消和解释器退出路径都要执行清理；
- 不在主线程的 `future.result()` 处理阶段关闭主线程连接；
- 不改变刷新结果统计、错误过滤、失效账号删除、进度更新和自动重新登录语义；
- wrapper 只负责资源清理，不吞掉 worker 原始异常。

该入口是本事件的核心修复点。即使后续 registry 的死线程回收作为防线存在，也不能以它替代显式的 worker `finally`。

### 4. 补齐其他后台线程边界

逐一审计所有能访问 SQLite store 的线程和 executor：

- `SnapshotWriter` 和 `TaskStateWriter` 保留现有 `close_current_thread()` finally 逻辑，改为调用统一 store 接口；
- quota watchdog 的线程函数在退出时调用 `AuthQuotaStore.close_current_thread()`；
- scheduler lane 的长期 executor 不要求每个任务都断开连接，以避免每个任务重复建立 SQLite 连接，但 lane 停止后必须先等待 executor 结束，再关闭 store；
- 图片任务、可编辑文件任务等长期 executor 若使用 `state_store`，连接数量应受其固定 worker 数限制，并在服务关闭时遵守相同的 stop/join/close 顺序；
- 只访问网络、JSON 配置或独立日志库的线程不强行接入 app.db cleanup，避免无关行为变化。

服务关闭顺序统一为：

```text
停止接收新任务
 -> 停止/取消后台 producer
 -> 等待 SQLite 使用线程和 executor 退出
 -> 调用各 store.close()
```

### 5. 诊断能力

在 debug/test 可用的诊断路径中提供：

- 各 SQLite store 当前 registry 连接数；
- 连接 owner 是否仍存活；
- 连接回收数量；
- 最近一次 registry cleanup 时间。

生产默认只记录计数和 store/db 名称，不记录 SQL 参数、账号 token、用户密钥、图片内容或响应正文。若项目已有 metrics/logging 约定，优先接入现有机制。

## 备选方案及取舍

### 仅在 `refresh_accounts()` 中关闭连接

这是最小补丁，但只覆盖当前已确认入口。`AuthQuotaStore`、未来新线程池或遗漏 cleanup 的后台线程仍可能把连接加入全局集合并长期持有。作为回归保护不够，因此不单独采用。

### 全局 SQLite 连接池

连接池可以设置硬上限，但会把连接借还、事务重置、跨线程使用、池关闭和超时处理引入所有 store。当前事件的根因是错误的线程生命周期，而不是并发连接上限不足。引入连接池会扩大改动面并改变锁等待行为，本次不采用。

### 单一共享 SQLite 连接加全局锁

内存占用最低，但会把账号、scheduler 和 quota 的并发读写都压到同一把锁，且要求重新审计所有 `execute()` 调用。当前业务已经存在 store 级事务锁，本次没有足够证据支持这种吞吐/延迟退化。

## 测试规格

### 单元测试

新增 `test/test_sqlite_connection_registry.py`，覆盖：

1. 同一线程重复 `connection()` 只创建一个连接。
2. `close_current_thread()` 关闭并移除当前线程连接，重复调用不报错。
3. 新线程创建连接后退出，下一次 `connection()` 会回收已退出线程的连接。
4. 多线程并发创建连接时登记集合不损坏，关闭后连接数为零。
5. 连接初始化失败或关闭失败时异常行为和清理计数可诊断。
6. `close()` 后再次使用的行为符合 store 当前约定，不静默复用已关闭连接。

### store 回归测试

扩展以下测试：

- `test/test_scheduler_state_store.py`：验证现有 writer close 行为仍然成立，并覆盖普通短生命周期线程；
- `test/test_quota_service.py`：验证 quota watchdog 退出后不保留连接；
- 新增 SQLite state store 的短生命周期 worker 测试，验证账号写入后线程连接已释放。

每个 store 都至少执行一次以下模式：

```text
for 300 rounds:
    create a one-worker ThreadPoolExecutor
    worker opens and uses the store connection
    wait for executor shutdown
assert connection count == baseline
```

该测试规模用于模拟线上数十小时刷新，而不是只验证一次 executor。

### 账号刷新集成测试

使用 99 个 stub token 和 10 个 worker，运行至少 30 个刷新周期的等效调用。验证：

- 刷新成功、失败、失效账号和取消路径结果不变；
- 每轮 executor 结束后 SQLite store 连接数回到固定基线；
- 连接数不会随周期数增长；
- 没有未处理的 `sqlite3.ProgrammingError`、`database is locked` 或 future 异常丢失。

### 并发与性能测试

- scheduler、quota、account refresh 同时运行时，事务结果保持正确；
- 保留当前 WAL 和 busy timeout 配置，比较修复前后的 SQLite 锁等待和请求错误率；
- 长期 executor 的连接数不超过其 worker 数加固定初始化连接；
- worker 退出后，`connection_count()` 和文件描述符回到可解释的固定范围。

## 线上验收

### 部署前

记录以下基线：

- Python PID、线程数、RSS、`RssAnon`、`RssFile`、`VmSwap`；
- `app.db` 和 `app.db-wal` FD 数；
- 账号刷新周期、账号数和每轮 worker 数；
- 服务日志中的 SQLite 错误计数。

### 部署后

由于旧进程已经持有泄漏连接，发布修复版本时需要重启容器一次；新代码不能安全地从旧 Python 进程中恢复旧连接的 owner 信息。重启后执行以下观察：

1. 连续观察至少 6 个账号刷新周期，建议覆盖 30 分钟以上；
2. 每轮刷新完成后记录 `app.db`/`app.db-wal` FD 数；
3. 验证 FD 数在长期 worker 基线附近波动，不再按周期增长；
4. 验证 RSS 在预热后无单调增长趋势，swap 不再持续增加；
5. 验证账号刷新、文本请求、图片任务、额度扣减和 scheduler 没有新增 SQLite 错误。

验收条件：

- 连续 6 轮刷新中，刷新结束后的 app.db 连接数不呈单调增长；
- 短生命周期刷新 worker 退出后，其连接在本轮结束或下一次 store 使用前被回收；
- `app.db` 和 `app.db-wal` FD 数与存活 SQLite 线程数相符，而不是与历史刷新次数相符；
- 30 分钟观察窗口内 RSS 和 swap 没有继续复制本次事件的线性增长曲线；
- 没有新增功能性错误或明显的 SQLite 锁等待回归。

swap 不要求重启后立即归零；验收看重启后是否停止增长以及 RSS 是否稳定。必要时用同样负载继续观察 2 小时确认趋势。

## 部署与回滚

### 部署

1. 合并实现和测试后构建目标分支镜像。
2. 停止旧容器并启动新镜像，确保后台 executor 和 watchdog 按 stop/join/close 顺序退出。
3. 记录重启前后的 PID、FD、RSS、swap 和 SQLite 错误计数。
4. 按线上验收窗口观察至少 6 个刷新周期。

### 回滚

若出现账号刷新失败、额度事务错误、scheduler 任务丢失或明显锁等待回归：

1. 停止新容器；
2. 回滚到上一个已验证镜像；
3. 保留 `app.db`、`logs.db` 和日志用于分析，不删除数据库文件；
4. 使用本 spec 的诊断指标确认是否为连接关闭时序问题，再修复后重新发布。

本修复不改变 schema 和业务数据，因此回滚不需要数据迁移。旧版本重新启动后仍可能复现连接增长，回滚只用于恢复业务可用性。

## 实施清单

- [ ] 新增 `SQLiteConnectionRegistry`，覆盖连接创建、owner 登记、死线程回收和关闭。
- [ ] 改造 `SQLiteStateStore` 使用 registry，并实现 `close_current_thread()`。
- [ ] 改造 `SchedulerStateStore` 使用 registry，保持 writer 清理行为。
- [ ] 改造 `AuthQuotaStore` 使用 registry，并补齐 watchdog 退出清理。
- [ ] 为 `AccountService.refresh_accounts()` 增加 worker `finally` cleanup。
- [ ] 审计长期 executor 的 stop/join/close 顺序。
- [ ] 增加 registry、store、account refresh 的回归测试和 300 轮增长测试。
- [ ] 执行目标分支测试、diff 检查和线上验收。
