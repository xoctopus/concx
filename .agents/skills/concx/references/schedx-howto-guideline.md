# schedx 指南

本文描述 `pkg/schedx` 的职责, 生命周期与选项约定.

模块路径: `github.com/xoctopus/concx/pkg/schedx`

## 职责

`Scheduler[T]` 是带排队与并发限制的 Job 执行器:

- 入队: `Push`
- 启动 worker: `Run` (内部创建 `nest.Nest`)
- 执行: `Job[T].Do` / `JobFunc[T]`
- 退出: `Close` → nest `Cancel`, 可选剩余任务回调

调度模式: FIFO (默认, 安全队列) / LIFO (安全栈).

## 生命周期

```
NewScheduler → Run → Push* → Close
```

约束:

- `Run` 只能成功一次; 再次 `Run` → `ERROR__SCHEDULER_RERUN`
- `Close` 后 `Push` → `ERROR__SCHEDULER_CANCELED`
- `Run` 之前可以 `Push` (任务先积压在队列)
- pending 达上限时 `Push` → `ERROR__REACH_MAX_PENDING`

推荐用法:

```go
s := schedx.NewScheduler(job, options...)
if err := s.Run(ctx); err != nil {
return err
}
defer func () { _ = s.Close() }()

if err := s.Push(ctx, item); err != nil {
// REACH_MAX_PENDING / SCHEDULER_CANCELED
}
```

## 选项

| 选项                       | 默认              | 含义                                             |
|----------------------------|-------------------|--------------------------------------------------|
| `WithMaxPending`           | `1`               | 待处理上限                                       |
| `WithoutPendingLimitation` | —                 | `maxPending = -1`, 不限                          |
| `WithParallel`             | `1`               | worker 数, 上限 `MaxParallel` (10000)            |
| `WithFifoScheduleMode`     | 默认              | FIFO                                             |
| `WithLifoScheduleMode`     | —                 | LIFO                                             |
| `WithCallback`             | —                 | 每个 job 结束后调用 `(T, error)`                 |
| `WithExitCallback`         | —                 | 关闭时剩余 pending 任务 + reason; 至多一次       |
| `WithCloseTimeout`         | `3s`              | 传给 nest shutdown; `0` 表示一直等到 worker 退出 |
| `WithoutDetached`          | 默认 **detached** | 见下节                                           |

## Detached 语义

默认 (未设 `WithoutDetached`):

- 弹出任务后用 `context.WithoutCancel(parent)` 执行
- job **不**跟随 scheduler / parent 的取消与 deadline
- 仍继承 context values (如 TraceID)

`WithoutDetached()`:

- job 使用 nest 的 dispatched ctx
- 可响应 `Close` / parent 取消

长任务若必须可被 `Close` 打断, 使用 `WithoutDetached`, 并在 `Job.Do` 内监听 `ctx.Done()`.

## Job 与回调

```go
schedx.JobFunc[T](func (ctx context.Context, v T) error { ... })

schedx.WithCallback(func (v T, err error) {
// err 含 Do 返回值; panic 时为 ERROR__SCHEDULER_JOB_PANICKED
})

schedx.WithExitCallback(func (pending []T, reason error) {
// Close / 取消时队列残留
})
```

panic 在 `do` 内 recover, 不会打垮 worker; 经 `WithCallback` 以 code error 暴露.

## 错误码

| Code                            | 何时                           |
|---------------------------------|--------------------------------|
| `ERROR__REACH_MAX_PENDING`      | pending 已满                   |
| `ERROR__SCHEDULER_RERUN`        | 重复 `Run`                     |
| `ERROR__SCHEDULER_CANCELED`     | 已关闭后 `Push`, 或退出 reason |
| `ERROR__SCHEDULER_JOB_PANICKED` | job panic                      |

关闭超时若来自 nest: `nest.ERROR__NEST_CLOSE_TIMEOUT` (经 `Close` 返回).

判定: `codex.IsCode(err, schedx.ERROR__...)`.

## 与 Nest 的关系

`Run` 时创建 nest, 并:

- `WithBeforeCloseFunc`: 周期性 `cond.Broadcast`, 唤醒空闲 worker
- `WithAfterCloseFunc`: 触发 `WithExitCallback`
- `WithShutdownTimeout`: 使用 `closeTimeout`
- `parallel` 次 `Spawn(run loop)`

宿主一般直接用 Scheduler; 只有「无队列、只要一组 goroutine」时用 `pkg/nest`.
详见 [nest-howto-guideline.md](nest-howto-guideline.md).

## 参考源码

- API: `pkg/schedx/schedx.go`
- 实现: `pkg/schedx/scheduler.go`, `pkg/schedx/option.go`
- 示例与约定: `pkg/schedx/scheduler_test.go`
