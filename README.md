# 本地任务编排与失败恢复引擎

一个用本地持久化状态驱动的 Go 任务编排引擎。它根据任务间的依赖关系进行
调度，对失败任务执行可配置的重试与超时，并保证进程中断重启后结果一致、
可解释。所有调度决策与状态迁移都会输出结构化日志。

- 依赖驱动调度：全部前置任务成功后后继才可执行；无依赖关系的任务并发推进。
- 开始执行**之前**拒绝非法定义：重复任务 ID、指向不存在任务的依赖、循环
  依赖（含自依赖），并给出可区分的失败原因。
- 可配置重试（线性退避、上限封顶）与按任务超时；可重试 / 不可重试失败
  区别对待；超预算 / 超时进入确定的终止状态。
- 同一次任务执行**至多一次成功生效**：单调度器（actor）模型 + 状态机约束，
  并发触发、重试都不会造成重复生效。
- 本地 JSON 状态文件（临时文件 + fsync + rename 原子写），版本号单调递增，
  回退提交与自相矛盾的快照被显式拒绝。
- **计划一致性**：快照带任务定义（依赖、超时、重试策略）的 SHA-256 摘要，
  重启时定义与落盘时不一致（任务增删或任何字段被改）会以独立的
  `plan_changed` 原因被明确拒绝；它与"状态自身损坏/回退"
  （`invalid_persisted_state`）是两种可区分的失败。
- **优雅停机**：`Shutdown` 后不再派发新任务，排空并正常提交所有在途尝试
  （成功 / `retry_wait` / 终止失败都有明确落盘归宿）；重启从落盘进度续跑，
  已完成任务绝不重跑，已排定的待办不丢失。
- **重试等待扛重启**：退避等待的是落盘的**绝对** `NextRunAt`。进程在等待
  窗口里被停掉，重启后已过期立即续跑、未过期按原时间点继续，反复重启也不会
  重新计时、不会把总等待无限拉长。
- 中断恢复：崩溃时在跑的任务有明确稳定的归宿（关闭为 `interrupted`，有
  预算则立即重投，否则确定性失败）；已完成任务重启后绝不重复执行。
- 时间全部抽象在 `Clock` 接口后：测试用 `FakeClock` 推进计时器，**不依赖
  任何真实等待**。

## 安装与快速体验

```bash
go run ./cmd/demo
```

demo 会构建一个菱形 DAG（`build -> {lint,test} -> package`）。它先让
`build` 失败一次进入退避等待，然后在等待窗口里**优雅停机**，再用同一状态
文件重启：日志能看到重启后按剩余时间重新挂计时器、在原 `NextRunAt` 到期时
续跑，最终全部成功且已完成任务不重跑。

## 核心抽象

```go
type TaskDef struct {
    ID      string
    Deps    []string      // 全部依赖成功后才可执行
    Timeout time.Duration // 单次尝试超时；0 表示不限制
    Retry   RetryPolicy
}

type RetryPolicy struct {
    MaxAttempts    int           // 总尝试次数（含首次）；<1 归一化为 1
    InitialBackoff time.Duration // 第 2 次尝试前的等待
    MaxBackoff     time.Duration // 退避上限（线性增长）
}

type Executor interface {
    Execute(ctx context.Context, taskID string) Result
}

type Result struct {
    OK        bool
    Retryable bool   // 仅在 OK=false 时有意义：是否允许重试
    Reason    string // 每次尝试的原因，都会落盘审计
}
```

存储与时间是接口：

```go
type Store interface {
    Load() (*Snapshot, error)          // 无快照时返回 ErrNoSnapshot
    Commit(snap *Snapshot) error       // 必须原子替换；拒绝版本回退
}
```

`FileStore`（JSON 原子写）与 `MemoryStore`（测试/内嵌）均已内置。

## 调度语义

1. **执行前校验（任何任务都不会先跑起来）**，按固定顺序返回首个错误：
   - 空 ID / 同一依赖重复列出 → `ReasonInvalidTask`
   - 任务 ID 重复 → `ReasonDuplicateID`
   - 依赖指向未定义任务 → `ReasonUnknownDep`
   - 依赖成环（含自依赖）→ `ReasonCycle`，错误信息附带完整环路径
2. 校验通过后加载（或初始化并落盘）快照，校验快照一致性，再做崩溃恢复。
3. 单一调度 goroutine 拥有全部可变状态；worker 与 `Trigger` 调用只通过事件
   通道与其通信，因此不存在两处同时把任务带出 `running` 的可能。
4. 每轮事件处理后执行一次调度扫描：
   - `pending`：依赖全部成功 → 派发；有依赖失败 → 合成 `deps_failed` 终止；
     否则继续等待；
   - `retry_wait`：退避计时器到期且依赖未失败 → 派发下一次尝试；
   - 并发度由 `Options.MaxConcurrent` 信号量限制；槽位不足时可运行任务
     被推迟，待下一个事件到来时再投递。
5. 上游失败沿依赖链确定性传播：后继任务不消耗执行器配额，直接以
   `deps_failed` 尝试记录进入终止失败，原因指向具体的失败上游。

## 重试与超时决策表

某次尝试结束后（在调度器这唯一一处提交）：

| 尝试结果 | 预算情况 | 下一状态 |
| --- | --- | --- |
| 成功 | — | `succeeded`（全机唯一一条成功尝试记录） |
| 失败 `Retryable=false` | — | `failed`（不重试，立刻终止） |
| 失败 `Retryable=true` | 已达 `MaxAttempts` | `failed`（原因为"预算耗尽 x/x"） |
| 失败 `Retryable=true` | 仍有预算 | `retry_wait`，退避 `InitialBackoff*(n-1)`，封顶 `MaxBackoff` |
| 超时 | 已达 `MaxAttempts` | `failed`（尝试类别为 `timeout`） |
| 超时 | 仍有预算 | `retry_wait`（超时同样受预算约束，不会无限重试） |

超时在引擎侧判定：超时触发即取消该尝试的 context，尝试以 `timeout` 关闭；
若执行器在此之后才返回结果，调度器以"当前尝试序号不匹配"丢弃该迟到回复，
不会产生第二次生效。

执行器 panic 被捕获为一次可重试失败，不会击穿引擎。

## 状态机

```
                ┌──────────────────────────────────────┐
                ▼                                       │
 pending ──► running ──成功──────────────────────► succeeded（终止）
   │           │  ▲                                   
   │           │  └──────────── retry_wait（退避到期） 
   │      可重试失败/超时且仍有预算                          
   │           ▼                                     
   │       retry_wait                                 
   │  依赖失败  │ 依赖失败                              
   ▼           ▼                                     
 failed ◄──────────────────── 预算耗尽 / 不可重试 / 超时耗尽（终止）
```

合法迁移白名单见 `allowedTransitions`；任何不在表中的迁移（含一切回退）
都会返回 `ReasonInvalidState` 错误，而不是被悄悄接受。

`Attempt` 是只追加的审计证据：序号连续、从 1 开始、不可改写；`running`
状态只允许最后一条尝试是"开放"的（无结束时间、无类别）。

## 持久化、停机与恢复规则

- 每次调度决策（派发、尝试结束、恢复调和）都会：状态版本号 +1 → 全量
  快照一致性校验 → `Store.Commit` 原子落盘。
- `Commit` 强制版本严格单调，旧版本 / 同版本提交直接拒绝（防状态回退）。
- 已 `succeeded` / `failed` 的任务在重启后原样保留，**永不重新执行**；
  无运行中任务时恢复过程是无副作用的 no-op，版本号不前进。

### 计划一致性（定义变更 vs 状态损坏）

每次初始化快照都会写入 `plan_digest`：对**归一化后**的全部任务定义
（ID 排序、依赖排序后取 SHA-256），覆盖范围为

- 任务集合（增 / 删任务）；
- 每个任务的依赖列表（增删、改依赖）；
- 每个任务的单次尝试超时 `Timeout`；
- 每个任务的重试策略 `MaxAttempts` / `InitialBackoff` / `MaxBackoff`。

重启加载时先做计划判定，再做快照内部一致性校验，两种失败严格区分：

| 情形 | 错误原因 | 含义 |
| --- | --- | --- |
| 任务集合或任一上述字段与落盘时不同 | `ValidationError(ReasonPlanChanged)`，即 `plan_changed` | 定义被改过，引擎拒绝拿旧进度续跑 |
| 快照自带摘要与当前定义吻合，但任务集合 / 状态内容自相矛盾 | `ValidationError(ReasonInvalidState)`，即 `invalid_persisted_state` | 状态被篡改或发生回退，不是定义问题 |
| 状态字段矛盾（多成功、序号断档、状态/尝试不符等） | `invalid_persisted_state` | 状态自身损坏 |

只有摘要**完全一致**（任务声明顺序、依赖列出顺序不影响摘要）才允许接着跑。
`plan_changed` 是一种"安全拒绝"：快照本身没坏，只是不再属于这份计划；处理
办法是用与落盘时相同的定义重启，或在确认可以放弃旧进度后清空状态文件重新
开始。引擎不会静默地用新定义套旧进度跑出矛盾结果。摘要本身缺失（被删改）
同样按 `invalid_persisted_state` 拒绝，而不是猜测性续跑。

### 优雅停机（Shutdown）

`Shutdown(ctx)` 在 `Run` 启动后调用：

1. 调度器进入 **draining**：之后不再派发任何新任务；新的 `Trigger` 立即
   返回 `ErrShutdownInProgress`，此前因依赖未满足而挂起的 Trigger 也以该
   错误被唤醒。
2. 已经在跑的尝试**不被打断**，允许它们正常结束；其结果按正常决策表提交：
   - 成功 → `succeeded`；
   - 可重试失败 / 超时且仍有预算 → `retry_wait`（带绝对 `NextRunAt`）；
   - 预算耗尽 / 不可重试 → `failed`。
   因此停机时不存在含糊的 `running`：每个在途任务都有明确稳定的落盘状态。
3. 尚未开始的任务保持 `pending`；`retry_wait` 任务保持其 `NextRunAt`。
   这些"已排定要做的事"不会因停机消失，下次启动照常推进。
4. `Shutdown` 阻塞到所有在途尝试都已结束并落盘（返回 `nil`）。若传入的
   `ctx` 先到期，则取消在途执行器的 context、按**崩溃式**退出：仍未结束的
   尝试保留为带开放尝试的 `running`，`Shutdown` 返回该 ctx 的错误
   （如 `context.DeadlineExceeded`），下次启动由恢复规则调和。`Shutdown`
   可重复调用，第二次起为空操作。

### 重试等待窗口跨重启

退避等待的目标是失败提交时刻 + 退避时长得到的**绝对时间点**
（`TaskState.NextRunAt`），它随 `retry_wait` 状态一起落盘；进程内存里的
计时器只是唤醒手段，重启后按落盘时间点重新挂一个：

- 重启时若 `now >= NextRunAt`（停机期间等待时间早就过了）→ 立刻具备运行
  资格，下一次调度扫描立即派发，**不额外等待**；
- 若 `now < NextRunAt` → 按**剩余时间** `NextRunAt - now` 重新挂计时器，
  到原来的那个时间点才续跑，不会把等待从头再计时；
- 反复起停只重复"重挂计时器"，`NextRunAt` 不变，总等待不会被重启次数拉长。

### 崩溃时在跑任务的归宿

`Run` 的 context 被外部直接取消（或被强制 kill / 机器重启）与停机 deadline
到期同等对待：一旦退出，调度器不再提交任何在途尝试结果，在跑任务保留为带
开放尝试的 `running`。下次 `New` 时按唯一确定规则调和：

1. 开放尝试以 `interrupted` 类别关闭并记录原因；
2. 若尝试预算未用尽 → `retry_wait`，`NextRunAt=恢复时刻`（立即具备再次
   运行资格，即 at-least-once 投递）；
3. 若预算已用尽 → 终止 `failed`，终止原因就是这次中断。

> 崩溃无法区分"执行器真的停了"与"执行器很慢但副作用已生效"，因此语义是
> at-least-once：中断后可能再次执行，但引擎保证**只有一次尝试能提交成功**
> （唯一成功记录、状态机白名单、单调度器）。需要严格 exactly-once 副作用
> 时，执行器应自行做幂等键保护。

## 触发（Trigger）

`Trigger(ctx, taskID)` 把一次显式触发交给调度器裁决：

- 引擎尚未 `Run` → `ErrEngineNotRunning`；
- 已请求优雅停机（draining）→ `ErrShutdownInProgress`（不接纳新工作，
  已挂起的调用也以该错误返回）；
- 任务已在 `running` / `retry_wait` / 终止态 → `*ErrTriggerIgnored`
  （幂等：绝不产生第二次执行）；
- 依赖尚未全部成功 → 调用方挂起，任务被派发或因上游失败终止时被唤醒；
- 依赖已全部成功 → 返回 nil，并由紧随其后的调度扫描派发。

## 本地验证

```bash
# 全量单测（含 -race 竞态检测）
go test ./... -race

# 带调度决策日志的详细输出（测试日志打印每次调度与状态迁移依据）
go test ./... -race -v

# 静态检查与格式化
go vet ./...
gofmt -l .   # 无输出即合格

# 端到端示例（真实文件落盘：退避等待中优雅停机 → 重启按原 deadline 续跑）
go run ./cmd/demo
```

手动验证三类新行为的要点：

- **停机 / 重启续跑**：日志中先出现 `graceful shutdown requested: no new
  dispatches`，停机后状态文件里在途任务为明确的 `succeeded` / `retry_wait` /
  `failed`，未开始任务为 `pending`；重启日志出现 `plan digest ... matches`，
  已完成任务不再出现 `DISPATCH`。
- **重试等待跨重启**：停机前记下 `next_run_at`；重启早于该时刻时日志为
  `waiting for retry until ... (Ns remaining); arming timer`，到点才
  `DISPATCH ... retry backoff elapsed`；重启晚于该时刻则首次调度扫描立即
  `DISPATCH`，没有任何额外等待。
- **计划一致性拒绝**：删掉状态文件重新跑一次生成快照后，改动任意任务的
  依赖 / 超时 / 重试字段（或增删任务）再启动，`New` 直接返回
  `validation failed (plan_changed)`；而手工篡改状态文件内容仍返回
  `validation failed (invalid_persisted_state)`，两者原因码不同。

测试覆盖的关键场景：

| 场景 | 测试 |
| --- | --- |
| 重复 ID / 未知依赖 / 自依赖 / 两、三节点环 / 重复依赖 | `TestValidationErrors` |
| 非法定义下零执行 | `TestValidationBeforeExecution` |
| 多次失败后成功、逐次原因记录、线性退避 | `TestRetrySucceedsOnThirdAttempt` |
| 不可重试立即终止 | `TestNonRetryableFailsImmediately` |
| 预算耗尽确定失败 | `TestRetryBudgetExhausted` |
| 超时重试至耗尽（FakeClock，零真实等待） | `TestAttemptTimeoutRetriedThenExhausted` |
| 无依赖任务并发、依赖严格先后、并发度上限 | `TestIndependentTasksRunConcurrently` / `TestMaxConcurrentRespected` |
| 上游失败传播、后继不执行、独立任务不受影响 | `TestUpstreamFailurePropagates` |
| 重复 / 并发 Trigger 不产生重复生效 | `TestDuplicateTriggerNoDoubleExecution` / `TestConcurrentTriggersAfterDepsProduceOneSuccess` |
| Trigger 提前调用挂起并在依赖成功后唤醒 | `TestTriggerParksUntilDepsSatisfied` |
| 崩溃在跑 → interrupted 后重试成功 | `TestRecoverRunningTaskRetries` |
| 崩溃在最后一次尝试 → 确定性失败 | `TestRecoverRunningTaskBudgetExhausted` |
| 已完成任务重启不重跑、版本不前进 | `TestRecoverCompletedTaskNotRerun` |
| 优雅停机排空在途尝试并提交成功、后继不派发、重启续跑 | `TestGracefulShutdownDrainsInFlightNoAmbiguity` |
| 停机时在途失败提交 retry_wait，重启到期后续跑 | `TestGracefulShutdownCommitsRetryWait` |
| 停机期间 Trigger 被拒 / 挂起 Trigger 被唤醒 | `TestTriggerRejectedDuringShutdown` |
| 停机 deadline 到期、执行器不配合 → 崩溃式退出可恢复 | `TestShutdownDeadlineExpiresUncooperative` |
| Shutdown 幂等、未 Run 时调用被拒 | `TestShutdownIdempotent` / `TestShutdownBeforeRunRejected` |
| 等待重试中停机，重启未到期按原 deadline 续跑 | `TestRetryWaitRestartBeforeDueKeepsOriginalDeadline` |
| 等待重试中停机，重启已到期立即跑 | `TestRetryWaitRestartAfterDueRunsImmediately` |
| 硬崩溃发生在等待窗口，重启行为一致 | `TestRetryWaitHardCrashSurvivesRestart` |
| 反复重启不重新计时、不拉长总等待 | `TestRepeatedRestartsDoNotExtendBackoff` |
| 计时器在途时其他事件不得提前派发（调度竞态回归） | `TestRetryNotDispatchedWhileTimerInFlight` |
| 依赖/超时/重试/退避/增删任务变更被拒（`plan_changed`） | `TestRestartRejectsChangedPlan` |
| 定义一致（含重排序）可正常续跑；摘要顺序无关 | `TestRestartAcceptsIdenticalPlan` / `TestPlanDigestStableAndOrderIndependent` |
| 摘要匹配但任务集合被破坏 / 摘要被清空 → `invalid_persisted_state` | `TestCorruptTaskSetWithMatchingDigestIsInvalidState` / `TestMissingDigestIsInvalidState` |
| 篡改 / 回退快照被拒绝 | `TestCorruptOrRolledBackSnapshotRejected` / `TestTamperedStateContentIsInvalidState` |
| Store 版本回退提交被拒绝 | `TestStoreRejectsVersionRollback` |
| 磁盘 JSON 落盘与重启续跑 | `TestFileStoreRoundTrip` |
| 矛盾快照（多成功、序号断档、状态/尝试不符等） | `TestValidateSnapshotRejectsContradictions` |

## 代码结构

```
orchestrator/
  types.go      状态、尝试、重试策略等领域类型
  errors.go     可区分原因的校验错误
  dags.go       执行前 DAG 校验（重复/未知依赖/环）
  clock.go      Clock 抽象、系统时钟、可推进的 FakeClock
  state.go      状态机白名单与快照一致性校验
  plan.go       任务计划摘要（SHA-256）与重启计划一致性判定
  store.go      Store 接口、FileStore（原子写）、MemoryStore
  recovery.go   重启时的 in-flight 调和
  shutdown.go   优雅停机（draining、排空、deadline 崩溃式退出）
  engine.go     Engine、New、读取 API
  runloop.go    单调度器事件循环与全部状态迁移
  execute.go    单次尝试执行、超时、依赖判定
  trigger.go    显式触发 API
cmd/demo/       端到端示例（优雅停机停在 retry_wait，再重启续跑）
```
