# schedx 指南

本文描述 `pkg/schedx` 的职责, 两种 Scheduler, 生命周期与选项约定.

模块路径: `github.com/xoctopus/concx/pkg/schedx`

## 选型

| 需求                                                        | 用                              |
|-------------------------------------------------------------|---------------------------------|
| 入队执行, 不关心单次返回值; 用 Callback / ExitCallback 观测 | `Scheduler[T]`                  |
| 每次 Push 拿到可等待的 `Result[Out]`                        | `RetrievableScheduler[In, Out]` |
| 只要一组受管 goroutine, 无队列                              | `pkg/nest`                      |

## 共享生命周期

```
New* → Run → Push* → Close
```

约束(两者相同):

- `Run` 只能成功一次; 再次 `Run` → `ERROR__SCHEDULER_RERUN`
- **必须先 `Run` 再 `Push`**; 否则 → `ERROR__SCHEDULER_NOT_RUNNING`
- `Close` / nest 取消后 `Push` → `ERROR__SCHEDULER_CANCELED`
- pending 达上限 → `ERROR__REACH_MAX_PENDING`
- 父 ctx 取消与手动 `Close` **不区分错误码**, 均为 `ERROR__SCHEDULER_CANCELED`(可 `codex.Wrap` 底层 cause)
- `Pending()` = **队列深度**(Pop 后 `--`), 不含正在执行的 Job

## Scheduler(fire-and-forget)

```go
s := schedx.NewScheduler(job, options...)
if err := s.Run(ctx); err != nil {
	return err
}
defer func() { _ = s.Close() }()

if err := s.Push(ctx, item); err != nil {
	// NOT_RUNNING / REACH_MAX_PENDING / SCHEDULER_CANCELED
}
```

### 选项

| 选项                       | 默认              | 含义                                                          |
|----------------------------|-------------------|---------------------------------------------------------------|
| `WithMaxPending`           | `1`               | 队列上限                                                      |
| `WithoutPendingLimitation` | -                 | `maxPending = -1`                                             |
| `WithParallel`             | `1`               | worker 数, 上限 `MaxParallel`                                 |
| `WithFifoScheduleMode`     | 默认              | FIFO                                                          |
| `WithLifoScheduleMode`     | -                 | LIFO                                                          |
| `WithCallback`             | -                 | 每个 job 结束后 `(T, error)`                                  |
| `WithExitCallback`         | -                 | 关闭时剩余队列任务 + reason; 至多一次                         |
| `WithCloseTimeout`         | `3s`              | nest shutdown; `WithCloseTimeout(0)` 表示一直等到 worker 退出 |
| `WithoutDetached`          | 默认 **detached** | 见下节                                                        |

### Job 与回调

```go
schedx.JobFunc[T](func(ctx context.Context, v T) error { ... })

schedx.WithCallback(func(v T, err error) {
	// err 含 Do 返回值; panic 时为 ERROR__SCHEDULER_JOB_PANICKED
})

schedx.WithExitCallback(func(pending []T, reason error) {
	// Close / 取消时队列残留; reason 为 SCHEDULER_CANCELED(可 wrap cause)
})
```

panic 在 worker 内 recover, 不打垮 loop.

## RetrievableScheduler(带 Result)

```go
s := schedx.NewRetrievableScheduler(
	schedx.RetrievableJobFunc[In, Out](func(ctx context.Context, in In) (Out, error) {
		return out, nil
	}),
	schedx.WithRetrievableMaxPending[In, Out](100),
	schedx.WithRetrievableParallel[In, Out](4),
)
if err := s.Run(ctx); err != nil {
	return err
}
defer func() { _ = s.Close() }()

ret, err := s.Push(ctx, in)
if err != nil {
	return err
}
out, err := ret.Result(ctx)
```

### Close 语义

1. 未完成的已返回 `Result` **立刻** `failed(SCHEDULER_CANCELED)`, 避免 `Result()` 卡死
2. 队列中 waiting 的任务 **不再执行**(丢弃); 对应 Result 同样 failed
3. Job.Do 是否立刻停取决于 Detached; **不是** Retrievable 对 Result 的契约
4. **无** Callback / ExitCallback: 完成走 `Result`; 结果处理在调用方

`Result(ctx)` 取消只放弃本次等待, 不取消 Job, 不改变调度器状态.

### 选项

| 选项                                  | 默认          | 含义                                            |
|---------------------------------------|---------------|-------------------------------------------------|
| `WithRetrievableMaxPending`           | `1`           | 队列上限                                        |
| `WithoutRetrievablePendingLimitation` | -             | 不限                                            |
| `WithRetrievableParallel`             | `1`           | worker 数                                       |
| `WithRetrievableFifoScheduleMode`     | 默认          | FIFO                                            |
| `WithRetrievableLifoScheduleMode`     | -             | LIFO                                            |
| `WithRetrievableCloseTimeout`         | `3s`          | 仅 nest 等 worker; Result 在 BeforeClose 已解锁 |
| `WithoutRetrievableDetached`          | 默认 detached | Job.Do 是否跟随调度器 cancel                    |

## Detached 语义(两者相同)

默认(未设 WithoutDetached):

- 弹出任务后用 `context.WithoutCancel(parent)` 执行
- job **不**跟随 scheduler / parent 的取消与 deadline
- 仍继承 context values

`WithoutDetached` / `WithoutRetrievableDetached`:

- job 使用 nest dispatched ctx, 可响应 `Close` / parent 取消

长任务若必须被 `Close` 打断, 使用 WithoutDetached, 并在 `Do` 内听 `ctx.Done()`.

## 错误码

| Code                            | 何时                                                      |
|---------------------------------|-----------------------------------------------------------|
| `ERROR__REACH_MAX_PENDING`      | pending 已满                                              |
| `ERROR__SCHEDULER_NOT_RUNNING`  | `Push` 在 `Run` 之前                                      |
| `ERROR__SCHEDULER_RERUN`        | 重复 `Run`                                                |
| `ERROR__SCHEDULER_CANCELED`     | 已关闭后 `Push`; ExitCallback / Result failed 的 shutdown |
| `ERROR__SCHEDULER_JOB_PANICKED` | job panic(Callback 或 Result)                             |

关闭超时若来自 nest: `nest.ERROR__NEST_CLOSE_TIMEOUT`(经 `Close` 返回).

判定: `codex.IsCode(err, schedx.ERROR__...)`.

## 与 Nest 的关系

`Run` 时创建 nest:

- `WithBeforeCloseFunc`: 周期性 `cond.Broadcast`; Retrievable 在此 `dismiss` 未完成 Result
- `WithAfterCloseFunc`: Scheduler 触发 `WithExitCallback`
- `WithShutdownTimeout`: `closeTimeout`
- `parallel` 次 `Spawn(worker loop)`

详见 [nest-howto-guideline.md](nest-howto-guideline.md).

## 参考源码

- API: `pkg/schedx/types.go`, `pkg/schedx/error.go`
- 实现: `pkg/schedx/scheduler.go`, `pkg/schedx/retrievable_scheduler.go`, `pkg/schedx/option.go`
- 测试: `pkg/schedx/scheduler_test.go`, `pkg/schedx/retrievable_scheduler_test.go`
- 包文档: `pkg/schedx/doc.go`
