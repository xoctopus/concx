/*
Package schedx provides a concurrent job scheduler with bounded pending queues
and graceful shutdown.

The core type is [Scheduler]. Callers enqueue work with [Scheduler.Push], start
worker goroutines with [Scheduler.Run], and shut down with [Scheduler.Close].
Jobs are handled by [Job] or [JobFunc]. Internally, [Scheduler.Run] creates a
nest to track workers and orchestrate cancellation.

Key features:
  - FIFO (default) or LIFO scheduling via [WithFifoScheduleMode] / [WithLifoScheduleMode]
  - Configurable concurrency ([WithParallel]) and pending limit ([WithMaxPending])
  - Per-job completion callback ([WithCallback]) and exit callback for remaining
    tasks ([WithExitCallback])
  - Safe close with optional timeout ([WithCloseTimeout]); default close timeout
    is 3s
  - By default, running jobs use context.WithoutCancel so they are detached from
    scheduler cancellation; use [WithoutDetached] to propagate cancel

Lifecycle:

	NewScheduler → Run → Push* → Close

[Scheduler.Run] may succeed only once. After [Scheduler.Close], [Scheduler.Push]
returns ERROR__SCHEDULER_CANCELED. Pushing when the pending limit is reached
returns ERROR__REACH_MAX_PENDING.

Usage:

	s := schedx.NewScheduler(
		schedx.JobFunc[int](func(ctx context.Context, v int) error {
			return nil
		}),
		schedx.WithMaxPending[int](100),
		schedx.WithParallel[int](8),
		schedx.WithCallback(func(v int, err error) {}),
	)
	if err := s.Run(ctx); err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	if err := s.Push(ctx, 42); err != nil {
		return err
	}

For structured concurrency without a job queue, use package nest directly.
*/
package schedx
