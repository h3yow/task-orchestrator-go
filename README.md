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
- 中断恢复：崩溃时在跑的任务有明确稳定的归宿（关闭为 `interrupted`，有
  预算则立即重投，否则确定性失败）；已完成任务重启后绝不重复执行。
- 优雅停机：`Shutdown` 后不再派发任何新任务（含重试），在途尝试要么跑完并
  正常提交，要么在宽限期结束后被取消并以 `interrupted` 关闭；停机前已排定
  的待办（pending、retry_wait 的绝对重试时刻）原样保留并可续跑。**排空的
  完成条件由调度器侧的在途计数判定**：只有每个已派发尝试的结果都已被调和
  并 `Commit` 落盘后计数才归零，因此多个任务几乎同时结束（包括宽限期内自然
  跑完与到期被强制取消）也不会提前收尾；停机返回后快照里不可能再出现
  `running`。
- 重试等待跨重启：等待窗口以**绝对时间点 `NextRunAt`** 落盘，重启后按原时刻
  重新挂表——已过点立即续跑，没到点继续等剩余时间，反复重启不会重新计时。
- 计划一致性：快照带任务定义的 SHA-256 指纹；启动时定义（任务增删、依赖、
  超时、重试策略）与落盘时不一致会被**明确拒绝**（`PlanMismatchError`），
  与"状态本身损坏/回退"的 `ValidationError(invalid_persisted_state)` 分开。
- 时间全部抽象在 `Clock` 接口后：测试用 `FakeClock` 推进计时器，**不依赖
  任何真实等待**。

## 安装与快速体验

```bash
go run ./cmd/demo
```

demo 会构建一个菱形 DAG（`build -> {lint,test} -> package`），其中 `build`
前两次失败、第三次成功。它先在第一次运行中途发起优雅停机（把 build 停在
retry_wait），随后从同一状态文件重启续跑，最后再用一份被改过的计划启动并
展示 `PlanMismatchError` 拒绝。

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

## 优雅停机与重启续跑

停机有两条入口，归宿不同但状态都明确、稳定：

| 停机方式 | 在跑尝试的处理 | 重启归宿 |
| --- | --- | --- |
| `Shutdown(ctx)`（优雅停机） | 立即进入 drain：不派发任何新任务（含退避到期的重试）；在途尝试继续跑，在 `ctx` 宽限期内跑完的结果**正常提交**（成功/失败/重试等待都落盘）；宽限期结束仍未返回的尝试被取消 context，以 `interrupted` 关闭 | 与崩溃恢复同一套规则 |
| `Run` 的 context 被取消 / 进程崩溃 | 不提交在途结果，任务保留为带开放尝试的 `running` | 启动时以 `interrupted` 关闭后按预算续跑或终止失败 |

`Shutdown` 的细节：

- **排空完成的判定（并发安全）**：引擎在调度器 goroutine 上维护一个在途
  尝试计数（派发时 +1，对应 attempt-done 事件被处理、结果完成状态迁移并
  `Commit` 后 -1）。计数在结果事件被调和**之前**绝不会下降——worker 先
  释放并发槽再投递事件的窗口不再被视为"已排空"。因此当多个任务在停机
  前后几乎同时跑完（无论自然跑完还是被宽限期到期取消）时，`Shutdown`
  都会等到**最后一个**结果确定并落盘才返回；另外 `drainComplete` 还会
  二次确认快照中没有任何 `running` 任务。
- 返回前 drain 期间产生的每个状态都已 `Commit` 落盘，所以停机后的快照里
  **不存在任何模糊状态**：每个任务要么 `succeeded`/`failed`（终止态），
  要么停在明确的等待态 `pending`/`retry_wait`，绝不会留下"running 但进程
  已停"这种没有归属的中间态（强制取消的尝试也已关闭并落盘）；`Run` 随后
  返回 `ErrGracefulShutdown`。
- 停机末尾的统计 `DrainStats()`（in-flight 及五种状态计数、快照版本）由
  快照直接算出，与 `Snapshot()`/`State()` 严格一致：排空返回时
  `InFlight == 0` 且 `Running == 0`，不可能出现"统计说没有在途、状态里
  却有任务在跑"的矛盾。`finishDrain` 在内部会校验这一不变量，违反时
  返回 `invalid_persisted_state` 错误而不是带着矛盾状态结束。
- **没有任何在途任务时排空立即完成**（任务只是 pending 或停在 retry_wait），
  `Shutdown` 立刻返回、版本号不前进。
- 停机期间 `Trigger` 一律返回 `ErrEngineNotRunning`，已挂起的 Trigger 同样
  被唤醒并得到该错误，不会借停机窗口派发任务。
- 停机前已经排定的事不会丢：尚未开始的任务保持 `pending`（其依赖关系还在
  快照里）；已进入 `retry_wait` 的任务保留**原来的绝对重试时刻**；下游任务
  在续跑后照常被调度。
- `Shutdown` 在 `Run` 之前调用是空操作；宽限期到期本身不会让 `Shutdown` 返回
  错误——它只触发强制取消，随后等待这些调和全部持久化（执行器应在 context
  取消后尽快返回）。

### 重试等待窗口跨重启的规则

任务进入 `retry_wait` 时，状态里存的是**绝对时间点** `NextRunAt = 失败提交时刻 + 退避时长`，而不是"还要等多久"。下次启动：

1. `NextRunAt` 已早于当前时间 → 立即具备运行资格，首次调度扫描即派发，不会
   再补等；
2. `NextRunAt` 仍在未来 → 按 `NextRunAt - now` 的**剩余时长**重新挂表，等到
   的是最初排定的那个时刻；
3. 因此无论中途重启多少次，总等待时长都等于策略算出的原始退避，不会被每次
   重启重新计时而无限拉长；
4. 被 `Shutdown` 强制取消（`interrupted`）的尝试同样进入 `retry_wait`，
   `NextRunAt` 取停机提交时刻（即重启后立即可跑，等同于崩溃后的 at-least-once
   投递）。

## 计划一致性（指纹校验）

每次初始化的快照都带：

- `plan_hash`：规范化任务定义的 SHA-256（hex）。参与指纹的字段是
  **ID、依赖集合、单次超时、重试策略**（最大尝试次数、初始退避、退避上限）；
  任务声明顺序、依赖列出顺序不影响指纹（规范化时排序），重试策略按
  `New` 中的同一规则归一化。
- `plan`：参与哈希的规范化任务副本，仅用于在拒绝时**逐条列出差异**。

重启 `New` 的判定顺序：

1. 快照没有指纹（指纹特性上线前的旧快照）→ 拒绝，`PlanMismatchError`，
   原因是"无法证明计划一致"；
2. 指纹与当前定义一致 → 放行（即使是停机/崩溃留下的中间状态，也先恢复再跑）；
3. 指纹不一致 → 拒绝，`PlanMismatchError`，错误中给出新旧两个哈希，并逐任务
   说明：任务新增、任务删除、依赖变更、超时变更、重试次数/初始退避/退避上限
   变更。

这与**状态损坏**严格区分：

| 失败 | 类型 | 含义 |
| --- | --- | --- |
| 定义与落盘计划不一致 | `*PlanMismatchError`（字符串原因 `plan_mismatch`） | 状态本身完好，但不是这份定义跑出来的，拒绝用旧进度硬跑 |
| 快照自相矛盾 / 字段非法 / 状态回退 | `*ValidationError`，`Reason=invalid_persisted_state` | 状态本身损坏，与计划是否变化无关 |
| Store 收到不高于已落盘版本的提交 | 内部版本回退错误（`Commit` 直接拒绝） | 防止任何形式的状态回退 |

## 持久化与恢复规则

- 每次调度决策（派发、尝试结束、恢复调和）都会：状态版本号 +1 → 全量
  快照一致性校验 → `Store.Commit` 原子落盘。
- `Commit` 强制版本严格单调，旧版本 / 同版本提交直接拒绝（防状态回退）。
- 重启加载时，快照必须与当前任务定义集合**完全一致**（任务数、ID），且
  通过全部内部一致性检查；否则 `New` 直接返回 `ReasonInvalidState`，引擎
  拒绝从中间/矛盾状态继续推进。
- **崩溃时在跑任务的归宿（唯一的模糊状态，规则确定）**：
  1. 开放尝试以 `interrupted` 类别关闭并记录原因；
  2. 若尝试预算未用尽 → `retry_wait`，`NextRunAt=恢复时刻`（立即具备
     再次运行资格，即 at-least-once 投递）；
  3. 若预算已用尽 → 终止 `failed`，终止原因就是这次中断。
- `Run` 的 context 被取消（非优雅停机）与进程崩溃**同等对待**：一旦停机，
  调度器不再提交任何在途尝试结果，在跑任务保留为带开放尝试的 `running`
  状态，下次启动仍由上述恢复规则决定归宿；尚未开始的任务保持 `pending`。
  需要"停机即交代清楚"时应使用 `Shutdown`（见上一节）。
- 已 `succeeded` / `failed` 的任务在重启后原样保留，**永不重新执行**；
  只有结果确实没定下来的中断任务（崩溃留下的开放 `running` 尝试，或优雅
  停机被强制取消记为 `interrupted` 的尝试）才按中断处理：有预算则重投、
  预算耗尽则确定性失败。无运行中任务时恢复过程是无副作用的 no-op，版本号
  不前进；停机时在宽限期内自然跑完并已落盘的成功/失败结果就是最终结果，
  重启不会重跑。

> 崩溃无法区分"执行器真的停了"与"执行器很慢但副作用已生效"，因此语义是
> at-least-once：中断后可能再次执行，但引擎保证**只有一次尝试能提交成功**
> （唯一成功记录、状态机白名单、单调度器）。需要严格 exactly-once 副作用
> 时，执行器应自行做幂等键保护。

## 触发（Trigger）

`Trigger(ctx, taskID)` 把一次显式触发交给调度器裁决：

- 引擎尚未 `Run` → `ErrEngineNotRunning`；
- 任务已在 `running` / `retry_wait` / 终止态 → `*ErrTriggerIgnored`
  （幂等：绝不产生第二次执行）；
- 依赖尚未全部成功 → 调用方挂起，任务被派发或因上游失败终止时被唤醒；
- 依赖已全部成功 → 返回 nil，并由紧随其后的调度扫描派发。

## 本地验证

```bash
# 全量单测（含 -race 竞态检测）
go test ./... -race

# 反复跑以确认并发停机场景的稳定性（所有时序由 FakeClock / 并发闸门控制，
# 不做任何真实等待）
go test ./orchestrator -race -run 'TestDrain|TestGraceful|TestShutdown|TestRecover|TestRetry' -count=30

# 带调度决策日志的详细输出（测试日志打印每次调度与状态迁移依据）
go test ./... -race -v

# 静态检查与格式化
go vet ./...
gofmt -l .   # 无输出即合格

# 端到端示例（含真实文件落盘与重启恢复）
go run ./cmd/demo
```

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
| 优雅停机：在途成功提交、未派发任务保持 pending、重启只跑一次 | `TestGracefulShutdownWaitsForInFlight` |
| 优雅停机宽限期到期：强制取消以 interrupted 落盘为 retry_wait，重启重放一次 | `TestGracefulShutdownForcesAfterDeadline` |
| 强制停机耗尽最后预算 → 确定性失败 | `TestGracefulShutdownForceExhaustsBudget` |
| 停机中 Trigger 被拒、挂起 Trigger 被释放 | `TestTriggersRejectedDuringDrain` |
| Run 之前 Shutdown 为空操作 | `TestShutdownBeforeRunIsNoOp` |
| 多任务并发、几乎同时完成时停机不提前收尾（并发闸门稳定复现旧竞态），停机后无 running、统计与状态一致、重启不重跑 | `TestDrainConcurrentTasksAllFinishNaturally` |
| 多任务并发被宽限期到期强制取消：全部调和落盘后才返回，重启各重放一次 | `TestDrainConcurrentTasksForceCancelled` |
| 多任务混合收尾：宽限期内自然完成与到期强制取消并存，结果都不丢不重 | `TestDrainMixedNaturalAndForcedFinish` |
| 单个在途任务：宽限期内自然完成 / 到期强制取消 | `TestDrainSingleInFlightNaturalAndForced` |
| 无在途任务（pending + retry_wait）停机立即完成、版本不前进、绝对重试时刻保留 | `TestDrainCompletesImmediatelyWhenNothingInFlight` |
| 重试等待中停机、在原时刻之前重启继续等剩余时间、下游不丢 | `TestRetryWaitSurvivesRestartBeforeDeadline` |
| 重启时退避已过点 → 立即续跑 | `TestRetryWaitElapsedBeforeRestartDispatchesImmediately` |
| 连续两次重启不重新计时、总等待不被拉长 | `TestRepeatedRestartsDoNotResetRetryWait` |
| drain 中在途尝试失败 → retry_wait 绝对时刻落盘并续跑 | `TestAttemptFailingDuringDrainCommitsRetryWait` |
| 任务增删 / 依赖 / 超时 / 重试策略变更被拒绝 | `TestRestartRejectsChangedPlan` |
| 计划不一致与状态损坏错误类型可区分 | `TestPlanMismatchDistinctFromCorruptState` |
| 声明顺序不同的等价计划正常续跑 | `TestSamePlanResumes` |
| 拒绝信息精确到任务与字段；无指纹旧快照被拒 | `TestMismatchDetailsAreSpecific` / `TestLegacySnapshotWithoutFingerprintRejected` |
| 篡改 / 回退快照被拒绝 | `TestCorruptOrRolledBackSnapshotRejected` |
| Store 版本回退提交被拒绝 | `TestStoreRejectsVersionRollback` |
| 磁盘 JSON 落盘与重启续跑 | `TestFileStoreRoundTrip` |
| 矛盾快照（多成功、序号断档、状态/尝试不符等） | `TestValidateSnapshotRejectsContradictions` |

## 代码结构

```
orchestrator/
  types.go      状态、尝试、重试策略、快照计划指纹字段
  errors.go     可区分原因的校验错误、ErrGracefulShutdown
  dags.go       执行前 DAG 校验（重复/未知依赖/环）
  plan.go       计划规范化、SHA-256 指纹、变更差异列举、一致性裁决
  clock.go      Clock 抽象、系统时钟、可推进的 FakeClock
  state.go      状态机白名单与快照一致性校验
  store.go      Store 接口、FileStore（原子写）、MemoryStore
  recovery.go   重启时的 in-flight 调和、retry_wait 计时器重建
  shutdown.go   Shutdown 优雅停机、调度器侧在途计数驱动的 drain 完成判定、DrainStats 统计
  engine.go     Engine、New 中的指纹写入/校验、读取 API
  runloop.go    单调度器事件循环、drain 闸门、全部状态迁移
  execute.go    单次尝试执行、超时、强制取消、依赖判定
  trigger.go    显式触发 API
cmd/demo/       端到端示例（优雅停机 → 重启续跑 → 计划变更被拒）
```
