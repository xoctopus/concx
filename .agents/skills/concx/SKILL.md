---
name: concx
description:
  - 说明如何用 `github.com/xoctopus/concx` 做受约束并发(生命周期 / 编排 / 通信 / 配方)
  - Nest: Spawn / Cancel / 统一生命周期
  - Scheduler: 入队 / 并发执行 / pending 限制 / 安全退出(fire-and-forget)
  - RetrievableScheduler: Push 返回 Result, Close 立刻解锁未完成 Result
  - chanx: Observer / Subject / 可取消值流
  - orch/pipe: 线性多阶段流水线配方(Push → Nodes → Result)
  - 当需要在宿主项目接入 concx, 选型三包或 orch 配方, 或排查关闭/超限错误时使用
---

# concx

- 选型与 Scheduler: [references/schedx-howto-guideline.md](references/schedx-howto-guideline.md)
- Nest 生命周期: [references/nest-howto-guideline.md](references/nest-howto-guideline.md)
- chanx 通信: [references/chanx-howto-guideline.md](references/chanx-howto-guideline.md)
- orch/pipe 配方: [references/orch-pipe-howto-guideline.md](references/orch-pipe-howto-guideline.md)
- 包文档: `go doc github.com/xoctopus/concx/pkg/{schedx,nest,chanx,orch,orch/pipe}`

## 选型

| 需求                                   | 用                                |
|----------------------------------------|-----------------------------------|
| 树形协程派生, 统一取消与退出           | `pkg/nest`                        |
| 任务排队 + 并行消化(不取回单次结果)    | `pkg/schedx.Scheduler`            |
| 任务排队 + 每次 Push 等 `Result`       | `pkg/schedx.RetrievableScheduler` |
| 协程间传值/多播, 可取消订阅            | `pkg/chanx`                       |
| 线性多阶段 + Node 内并行 Job(固定约定) | `pkg/orch/pipe`                   |
| 编排内部已用 Nest                      | 一般不必再包一层 Nest             |

积木(nest / schedx / chanx)可自由组合; **orch** 是约定死的配方层, 不是第四能力轴.

## 最小 Scheduler

```go
s := schedx.NewScheduler(
	schedx.JobFunc[int](func(ctx context.Context, v int) error {
		return nil
	}),
	schedx.WithMaxPending[int](100),
	schedx.WithParallel[int](8),
)

_ = s.Run(ctx)   // 必须先 Run
_ = s.Push(ctx, 1)
_ = s.Close()
```

## 最小 RetrievableScheduler

```go
s := schedx.NewRetrievableScheduler(
	schedx.RetrievableJobFunc[int, int](func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	}),
	schedx.WithRetrievableMaxPending[int, int](100),
)

_ = s.Run(ctx)
defer func() { _ = s.Close() }()

ret, err := s.Push(ctx, 21)
if err != nil {
	return err
}
out, err := ret.Result(ctx)
_ = out
```

`Close` 会使未完成的 `Result` 立刻失败(`ERROR__SCHEDULER_CANCELED`), 队列中未执行任务丢弃. 详情见 [schedx-howto-guideline.md](references/schedx-howto-guideline.md).

## 最小 Nest

```go
n := nest.New(ctx, nest.WithShutdownTimeout(5*time.Second))
_ = n.Spawn(func(ctx context.Context) { /* 响应 ctx.Done() */ })
n.Cancel(nil)
<-n.Done()
```

## 最小 chanx

```go
obs := chanx.NewNotifiableObserver[int]()
go func() {
	obs.Send(1)
	obs.CancelCause(nil)
}()
for v := range obs.Value() {
	_ = v
}
```

## 最小 pipe

```go
p := pipe.New(ctx,
	pipe.NewNode(pipe.WithJobs(schedx.JobFunc[string](stepA))),
	pipe.NewNode(pipe.WithJobs(jobB1, jobB2)), // 同条目并行
	pipe.NewNode(pipe.WithJobs(schedx.JobFunc[string](stepC))),
)
defer func() { _ = p.Close() }()

_ = p.Push(ctx, "item")
v := <-p.Result()
_ = v
<-p.Done()
```
