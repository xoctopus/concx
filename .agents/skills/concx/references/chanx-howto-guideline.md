# chanx 指南

本文描述 `pkg/chanx`：协程间通信范式（可取消的值流 / 多播）。

模块路径: `github.com/xoctopus/concx/pkg/chanx`  
（`github.com/xoctopus/x/chanx` 已 archived，请迁到本包。）

## 职责

- `NotifiableObserver`：可 `Send` / `Value()` 接收，直到 `CancelCause`
- `Subject`：向多个订阅者 fan-out；`Observe` / `Subscribe`
- 生命周期：`Done` / `Err`；`CancelCause(nil)` → `ErrCompleted`

## 最小用法

```go
obs := chanx.NewNotifiableObserver[int]()
go func() {
	obs.Send(1)
	obs.CancelCause(nil)
}()
for v := range obs.Value() {
	_ = v
}

sub := &chanx.Subject[int]{}
o := sub.Observe()
sub.Send(1)
sub.CancelCause(nil)
<-o.Done()
```

## 与三轴关系

| 需求                   | 包       |
|------------------------|----------|
| 传值 / 多播 / 订阅结束 | `chanx`  |
| 树形派生与统一退出     | `nest`   |
| 任务排队与并行消化     | `schedx` |

组合示例：在 `Observe` 循环里 `scheduler.Push`（应用侧组合，非本包职责）。

## 参考

- `pkg/chanx/doc.go`
- `ExampleNotifiableObserver` / `ExampleSubject`
