# nest 指南

本文描述 `pkg/nest` 的职责, API 与关闭约定.

模块路径: `github.com/xoctopus/concx/pkg/nest`

## 职责

`Nest` 是受管控, 可派生的 goroutine 生命周期边界:

- 用 `sync.WaitGroup` 跟踪 `Spawn` 出的 goroutine
- 统一生命周期: parent 取消或主动 `Cancel`
- 可选 shutdown timeout, 避免关闭永久阻塞

实现了 `context.Context` (面向 dispatched 侧); 另提供 `Parent()` / `Children()`.

## 双 Context

| 方法         | 含义                                           |
|--------------|------------------------------------------------|
| `Parent()`   | 构造时传入的 inherited ctx (元数据 / 外部信号) |
| `Children()` | 内部 `WithCancelCause` 得到的 dispatched ctx   |

`Spawn` 的 worker 收到的是 **dispatched** ctx.
inherited 或 dispatched 任一 Done, 都会触发内部 `Cancel`.

## 生命周期

```
nest.New → Spawn* → Cancel → Done / Err
```

```go
n := nest.New(ctx,
	nest.WithShutdownTimeout(5*time.Second),
	nest.WithBeforeCloseFunc(func(c context.Context) {
		// Cancel 流程开始前; c 会在 worker 结束或超时后 Done
	}),
	nest.WithAfterCloseFunc(func(err error) error {
		return nil
	}),
)

if err := n.Spawn(func(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-work:
	}
}); err != nil {
	// ERROR__NEST_CLOSED: 已 Cancel 后再 Spawn
}

n.Cancel(cause)
<-n.Done()
err := n.Err()
```

## 选项

| 选项                  | 含义                                           |
|-----------------------|------------------------------------------------|
| `WithShutdownTimeout` | `Cancel` 等待 WaitGroup 的上限; `0` 表示一直等 |
| `WithBeforeCloseFunc` | 取消 dispatched 之前                           |
| `WithAfterCloseFunc`  | worker 退出流程结束后; 返回值并入 `Err()`      |

## 错误码

| Code                        | 何时                            |
|-----------------------------|---------------------------------|
| `ERROR__NEST_CLOSED`        | 已 Cancel 后 `Spawn`            |
| `ERROR__NEST_CLOSE_TIMEOUT` | shutdown 超时仍有 worker 未退出 |

## 与 schedx

`pkg/schedx.Scheduler` 在 `Run` 里创建 Nest 托管 worker loop.
需要队列 / pending / Job 回调时用 schedx; 只要"一组受管 goroutine"时直接用本包.

详见 [schedx-howto-guideline.md](schedx-howto-guideline.md).

## 参考

- API: `pkg/nest/nest.go`
- 示例: `pkg/nest/nest_test.go` (`ExampleNest`)
