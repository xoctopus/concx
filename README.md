# concx

受约束并发工具箱：生命周期、编排、通信。

| 包 | 轴 | 用途 |
|----|-----|------|
| [`pkg/nest`](pkg/nest) | 生命周期 | 受管派生、Spawn/Cancel、优雅退出 |
| [`pkg/schedx`](pkg/schedx) | 编排 | 任务队列、并行/串行、FIFO/LIFO、pending |
| [`pkg/chanx`](pkg/chanx) | 通信 | Observer/Subject、可取消值流 |

`github.com/xoctopus/x/chanx` 已 archived，请使用本仓库 `pkg/chanx`。

定位与扩展闸门见 [`pkg/todo.md`](pkg/todo.md).
