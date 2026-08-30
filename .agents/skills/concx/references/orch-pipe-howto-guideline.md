# orch/pipe 指南

本文描述 `pkg/orch/pipe` 的定位, 约定与用法.

模块路径: `github.com/xoctopus/concx/pkg/orch/pipe`

## 定位

`pipe` 是 **orch 配方**, 不是第四能力轴:

```
积木: nest / chanx / schedx
配方: orch/pipe(及未来其它 orch/*)
```

用三包拼好的线性流水线: `Push → node₀ → ... → nodeₙ → Result`.

- 需要自定义排队/背压/通信细节 → 直接用三包
- 需要"多阶段 + Node 内并行 Job"的固定范式 → 用 `pipe`

对外 **不** 暴露 `chanx`; Job 类型用 `schedx.Job` / `JobFunc`(不 re-export).

## 约定(写死, 无逃生舱)

| 约定               | 行为                                                           |
|--------------------|----------------------------------------------------------------|
| Node 间            | 线性; 成功才转发下一 Node                                      |
| Node 内多 Job      | 同 `v` **全并行**(并发度 = `len(jobs)`)                        |
| `len(jobs)==1`     | 直调, 无扇出开销                                               |
| 失败               | fail-fast: 取消同组其余 Job, **不**转发                        |
| 条目并发 / pending | 每 Node 内部 Scheduler 固定 `parallel=1`, FIFO, `maxPending=1` |

串行步骤: 合成一个 `Job`, 或拆成多个 Node.
要调条目并行/背压: 回三包自拼, 不要期望 pipe 开放 Scheduler 选项.

## 生命周期

```
pipe.New(ctx, NewNode(...), ...) → Push* → Result → Close → Done
```

```go
p := pipe.New(ctx,
	pipe.NewNode(
		pipe.WithName[string]("validate"),
		pipe.WithJobs(schedx.JobFunc[string](validate)),
	),
	pipe.NewNode(
		pipe.WithName[string]("enrich"),
		pipe.WithJobs(enrichA, enrichB), // 同条目并行
	),
	pipe.NewNode(
		pipe.WithJobs(schedx.JobFunc[string](persist)),
	),
)
defer func() { _ = p.Close() }()

_ = p.Push(ctx, item)
v := <-p.Result()
<-p.Done() // Close 或父 ctx 取消后关闭
```

约束:

- 至少需要一个 Node; 每个 Node 至少一个 Job
- `Result` 在 `New` 时已订阅好; 可先 `Push` 再读(内部已 bridge)
- 父 `ctx` 取消会触发 `Close`
- `Close` 后 `Push` → `schedx.ERROR__SCHEDULER_CANCELED`
- `Close` 后 `Done` 关闭; `Result` 随后关闭
- 阶段间 `maxPending=1`: 上游过快且下游忙时, bridge 可能丢拍(`REACH_MAX_PENDING` 被忽略)

## API

| API                      | 作用                                                    |
|--------------------------|---------------------------------------------------------|
| `New(ctx, nodes...)`     | 装配并启动                                              |
| `NewNode(opts...)`       | 配置 Node(惰性, 不启动)                                 |
| `WithJobs(jobs...)`      | 追加 Job (多个时同条目并发); 可多次调用, 最终合并为一组 |
| `WithName(name)`         | 调试标签                                                |
| `Push`                   | 投入首 Node                                             |
| `Result() <-chan T`      | 末 Node 成功产出                                        |
| `Done() <-chan struct{}` | 管线停机信号                                            |
| `Close()`                | 关闭                                                    |

## 拓扑示意

```
Push(v)
  → Node₀ Scheduler → jobs (1 或并行 N) → Subject
  → bridge Observe→Push
  → Node₁ ...
  → Result(v)   // 仅全路径成功
```

## 与三包的关系

| 包       | 在 pipe 中的角色                                     |
|----------|------------------------------------------------------|
| `schedx` | 每 Node 的 ingress 队列 + Job 执行                   |
| `chanx`  | Node 间 egress(内部 Subject; 用户只碰 Result/Done)   |
| `nest`   | Node 内多 Job 扇出与 fail-fast 取消                  |

选型对照见主 skill: [SKILL.md](../SKILL.md).
积木细节: [schedx-howto-guideline.md](schedx-howto-guideline.md), [nest-howto-guideline.md](nest-howto-guideline.md), [chanx-howto-guideline.md](chanx-howto-guideline.md).

## 参考源码

- 包说明: `pkg/orch/doc.go`
- API / 实现: `pkg/orch/pipe/pipe.go`
- 示例: `pkg/orch/pipe/example_test.go`
- 测试: `pkg/orch/pipe/pipe_test.go`
