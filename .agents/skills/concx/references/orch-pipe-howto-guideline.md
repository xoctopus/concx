# orch/pipe 指南

本文描述 `pkg/orch/pipe` 的定位, 约定与用法.

模块路径: `github.com/xoctopus/concx/pkg/orch/pipe`

## 定位

`pipe` 是 **orch 配方**, 不是第四能力轴:

```
积木: nest / chanx / schedx
配方: orch/pipe (及未来其它 orch/*)
```

线性多阶段流水线, 每次 Push 拿到可等待的 `Result[Tail]`:

```
Build → Run → Push* → Result → Close
```

- 需要自定义排队/背压/通信细节 → 直接用三包
- 需要 "Head→Tail 类型推进 + Node 内并行 Job" 的固定范式 → 用 `pipe`

对外不暴露 `chanx`. 阶段 Job 用本包 `TransformJob` / `TransformFunc`.

## 约定 (写死)

| 约定             | 行为                                                                               |
|------------------|------------------------------------------------------------------------------------|
| 编排             | `From*` → `Then` / `Parallel`* → `End*` → `Build`                                  |
| Node 间          | 线性; 成功才转发下一 Node                                                          |
| Node 内 Parallel | 同条目全并行, fail-fast, summary 合并                                              |
| 准入             | 内部 `RetrievableScheduler`: FIFO; `MaxPending` / `Parallel` / `CloseTimeout` 可配 |
| `Pending()`      | **准入队列深度** (Pop 后 `--`), 不含阶段内在飞                                     |
| Close            | 未完成 Result 立刻失败 (`PIPELINE_CANCELED`); 丢弃队列; 停阶段泵                   |

`Parallel` = 同时在管线内的票数 (每票占一个 worker 直到 Tail).

## 生命周期

```go
p := pipe.FromJob[string, string, string](
	"validate",
	pipe.TransformFunc[string, string](validate),
).Then(
	"enrich",
	pipe.TransformFunc[string, string](enrich),
).EndJob(
	"persist",
	pipe.TransformFunc[string, string](persist),
).Build(
	pipe.WithMaxPending(10),
	pipe.WithParallel(8),
)

_ = p.Run(ctx) // 必须先 Run
defer func() { _ = p.Close() }()

ret, err := p.Push(ctx, item)
if err != nil {
	return err
}
out, err := ret.Result(ctx)
_ = out
```

约束:

- 必须先 `Run` 再 `Push`; 否则 `ERROR__PIPELINE_NOT_RUNNING`
- `Close` / 取消后 `Push` → `ERROR__PIPELINE_CANCELED`
- pending 达上限 → `ERROR__REACH_MAX_PENDING`
- Job 失败 → Result 带 `ERROR__JOB_FAILED` (或 panic 码); 不转发后续 Node
- `Result(ctx)` 的 ctx 取消只放弃本次等待, 不单独取消管线票 (Close 会解堵)

## API

| API                                                       | 作用                         |
|-----------------------------------------------------------|------------------------------|
| `FromJob` / `FromUniversalJobs`                           | 起编排                       |
| `Then` / `Parallel`                                       | 追加阶段                     |
| `EndJob` / `EndJobs`                                      | 钉死 Tail, 进入 Build        |
| `Build(opts...)`                                          | 应用选项, 得到 `Scheduler`   |
| `Run` / `Push` / `Pending` / `Close`                      | 运行面 (同 Retrievable 形状) |
| `WithMaxPending` / `WithParallel` / `WithShutdownTimeout` | 选项                         |

## 拓扑示意

```
Push(Head)
  → Retrievable admission (drive)
  → origin → Node₀ → ... → Nodeₙ → finale
  → ticket.finish → drive returns Tail
  → Result[Tail]
```

## 与三包的关系

| 包       | 在 pipe 中的角色                            |
|----------|---------------------------------------------|
| `schedx` | 准入 RetrievableScheduler + per-Push Result |
| `chanx`  | 阶段间 flight (内部; 用户不碰)              |
| `nest`   | 阶段泵生命周期; ParallelNode 内扇出         |

选型对照见主 skill: [SKILL.md](../SKILL.md).
积木细节: [schedx-howto-guideline.md](schedx-howto-guideline.md), [nest-howto-guideline.md](nest-howto-guideline.md), [chanx-howto-guideline.md](chanx-howto-guideline.md).

## 参考源码

- 包说明: `pkg/orch/pipe/doc.go`
- 运行时: `pkg/orch/pipe/pipe.go`
- 编排 DSL: `pkg/orch/pipe/orch.go`
- Node: `pkg/orch/pipe/node.go`
- 示例: `pkg/orch/pipe/example_test.go`
- 测试: `pkg/orch/pipe/pipe_test.go`
