# 本地任务编排与失败恢复引擎

一个用**本地持久化状态**驱动 DAG 任务调度、并发执行、重试/超时与重启恢复的 Go 库。
所有调度决策都由已持久化的状态机推出，因此并发执行、失败重试与进程重启后的结果
一致且可解释。

模块路径：`github.com/h3yow/task-orchestrator-go`，核心包为 `orchestrator`。

---

## 1. 快速开始

```go
plan, err := orchestrator.BuildPlan("run-1", []orchestrator.TaskDef{
    {ID: "download", Run: download},
    {ID: "transform", Deps: []string{"download"},
        Retry: orchestrator.RetryPolicy{MaxAttempts: 3, Backoff: 100 * time.Millisecond},
        Timeout: 5 * time.Second,
        Run: transform},
})
if err != nil { /* 重复ID / 缺失依赖 / 循环依赖，见第 2 节 */ }

store, _ := orchestrator.NewFileStore("./state")
eng, err := orchestrator.NewEngine(plan, orchestrator.Options{
    Store: store, Clock: orchestrator.NewRealClock(), Logger: log.Default()})
if err != nil { /* 恢复失败 / 状态损坏，见第 5 节 */ }

err = eng.Run(context.Background())
// 全部成功 => nil；否则 *orchestrator.RunError，逐任务给出状态与原因
```

本地验证：

```bash
go build ./...
go vet ./...
go test -race -count=1 ./...           # 全部单测（含竞态检测），无真实等待
go run ./cmd/demo                      # 演示：重试 + 状态持久化
go run ./cmd/demo                      # 再次运行：已完成任务不会重复执行
```

---

## 2. 调度语义

- **依赖门控**：一个任务只有在它的**全部**依赖都 `succeeded` 后才进入 `ready`，
  随后被调度执行。没有依赖关系的任务**并发**推进（可用 `Options.MaxConcurrency` 限流）。
- **开始执行前的拒绝**：`BuildPlan` 在任何任务执行之前完成全部静态校验，错误可以
  用 `errors.Is` 区分：

  | 错误哨兵 | 触发条件 |
  | --- | --- |
  | `ErrDuplicateTask` | 同一任务标识重复定义（含空标识） |
  | `ErrMissingDependency` | 依赖指向图中不存在的任务 |
  | `ErrCircularDependency` | 存在环（含自依赖），错误信息附带环路径 |

- **失败传播**：任一依赖进入 `failed`/`skipped`，尚未尝试过的后继任务确定性地
  级联进入 `skipped`（`reason=dependency_failed`），**永远不会被执行**；
  与失败分支互不依赖的任务照常并发执行直至成功。
- 调度顺序基于 Kahn 拓扑排序并保持注册顺序稳定，调度依据通过 `Logger` 打印，例如：
  `[scheduler] task=b decision=ready all 1 dependencies succeeded -> ready`。

---

## 3. 重试与超时语义

- 每个任务独立配置 `RetryPolicy{MaxAttempts, Backoff, TimeoutIsRetryable}` 与
  `Timeout`。`MaxAttempts` 是**包含首次在内的总尝试次数**；`Backoff=0` 表示立即重试。
- 任务返回普通 `error` => **可重试失败**；用 `orchestrator.NonRetryable(err)`
  包装（或让错误实现 `IsPermanent() bool`）=> **不可重试失败**。两类原因分别记录为
  `retryable_error` 与 `non_retryable_error`。
- 尝试超过 `Timeout` 时立即得到确定结论：取消该尝试的 context，记录
  `reason=timeout`。超时是否消耗重试预算由 `TimeoutIsRetryable` 决定；
  不再重试时直接终止为 `failed(timeout)`。
- 超过尝试上限 => `failed(retries_exhausted)`。**每一次尝试**都在
  `TaskState.Records` 中留下一条记录（尝试序号、起止时间、错误文本、原因、是否超时），
  失败原因可逐条解释。
- **至多一次成功生效**：一个任务在同一时刻只有一个尝试在执行；
  `running` 期间的并发/重复触发返回 `ErrAlreadyRunning`，对 `succeeded` 的触发返回
  `ErrAlreadySucceeded`，均不会造成第二次执行。任务函数本身若有外部副作用，
  使用方应保证其幂等，以覆盖“进程在副作用提交与状态落盘之间被杀死”这一理论窗口。

时间由可注入的 `Clock` 决定；单测使用 `FakeClock` 手动推进时间，**完全不依赖真实
sleep**（任务为了制造并发重叠而做的毫秒级 sleep 除外，不影响调度正确性）。

---

## 4. 任务状态机

```
                依赖全部成功
 pending ───────────────────────► ready ──► running ──成功──► succeeded (终态)
    │                                ▲          │
    │ 依赖 failed/skipped            │          ├─不可重试错误/超时(不可重试)──► failed (终态)
    ▼                                │          ├─尝试次数耗尽─────────────────► failed (终态)
 skipped (终态)                      └─backoff──┤
        ▲                                   retrying
        └────────────────── 依赖失败的级联传播（仅对未尝试的任务）
```

| 状态 | 含义 | 允许的迁移 |
| --- | --- | --- |
| `pending` | 未就绪，未开始 | → `ready` / `skipped` |
| `ready` | 依赖已满足，可运行 | → `running` |
| `running` | 有一个尝试在执行（**已先持久化再调用任务函数**） | → `succeeded`/`failed`/`retrying` |
| `retrying` | 可重试失败，等待退避 | → `ready` |
| `succeeded` | 终态，成功恰好生效一次 | 不再迁移，重启后不重跑 |
| `failed` | 终态：不可重试 / 超时终止 / 次数耗尽 | 不再迁移 |
| `skipped` | 终态：依赖未成功 | 不再迁移 |

持久化**只允许正向迁移**；每次迁移都在调用外部代码前后建立明确的落盘边界：
`ready/running` 先落盘后执行，结果（成功/失败/退避）落盘后再唤醒调度。

---

## 5. 持久化与重启恢复

- `Store` 接口（`Load`/`Save`）有两种实现：
  - `FileStore`：每个 run 一个 `<dir>/<runID>.json`，写入采用
    **临时文件 + fsync + 同目录 rename**，崩溃只能看到旧快照或新快照，不会出现半写文件；
  - `MemoryStore`：供测试与内嵌使用。
- `PlanHash` 是任务图与重试/超时配置（ID、依赖边、`MaxAttempts`/`Backoff`/
  `Timeout` 等，不含函数字面量）的确定性指纹。重启时计划不一致会被显式拒绝：
  `ErrPlanMismatch`，而不会拿旧状态套新图继续跑。
- 每次 `NewEngine` 都会先 `Load`、再做**全量一致性校验** `validateSnapshot`，
  任何回退或自相矛盾都以 `ErrInvalidState` 显式拒绝，例如：
  未知/缺失任务、非法状态、尝试计数回退、记录序号错乱、`succeeded` 末条记录是失败、
  `skipped` 却没有失败依赖、任务已运行而依赖并未全部成功、JSON 损坏等。
- **中断中任务的稳定归宿**（在 `NewEngine` 恢复阶段一次性完成并持久化）：
  - `running`：把被打断的尝试补记一条 `reason=interrupted` 记录——
    尝试预算仍允许时迁移到 `ready` 并由调度器重试；预算已尽时确定性地终止为
    `failed(retries_exhausted)`。
  - `retrying`：退避等待不跨进程保留，直接迁移回 `ready`。
  - `pending`：若依赖已 `failed/skipped`，级联 `skipped`；否则保持等待。
  - 终态（`succeeded/failed/skipped`）原样保留，**已完成任务绝不重复执行**。

恢复结论同样通过日志打印（`[recovery] task=... decision=...`），单测覆盖了
“running 中断后续跑成功”“末次尝试中断后终态失败”“计划变更拒绝”“损坏文件拒绝”
“自相矛盾状态拒绝”与 FileStore 真实落盘往返。

---

## 6. API 摘要

| 符号 | 说明 |
| --- | --- |
| `BuildPlan(runID, defs)` | 静态校验并构建不可变计划 |
| `NewEngine(plan, Options)` | 加载/校验快照并执行恢复 |
| `(*Engine).Run(ctx)` | 并发跑到全部终态；失败返回 `*RunError` |
| `(*Engine).TriggerTask(ctx, id)` | 手动运行单个就绪任务；重复/并发触发返回可区分错误 |
| `(*Engine).Wait(ctx)` / `State(id)` / `Snapshot()` / `Close()` | 观察与生命周期 |
| `NonRetryable(err)` / `IsNonRetryable(err)` | 永久失败标记 |
| `NewFileStore(dir)` / `NewMemoryStore()` | 状态存储 |
| `NewRealClock()` / `NewFakeClock(t)` | 时间抽象 |
