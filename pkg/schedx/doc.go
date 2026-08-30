/*
Package schedx provides concurrent job schedulers with bounded pending queues
and graceful shutdown.

# Scheduler

[Scheduler] is fire-and-forget: enqueue with [Scheduler.Push], start workers
with [Scheduler.Run], shut down with [Scheduler.Close]. Jobs are [Job] /
[JobFunc]. Completion is observed via [WithCallback]; remaining queued items on
exit via [WithExitCallback].

# RetrievableScheduler

[RetrievableScheduler] returns a [Result] per successful [RetrievableScheduler.Push].
Callers wait with [Result.Result]. There is no completion Callback - Result is
the completion channel. On Close, all unfinished Results are failed immediately
so waiters do not block; queued work is discarded and not executed.

# Shared lifecycle

	New* → Run → Push* → Close

Constraints (both schedulers):

  - [Scheduler.Run] / [RetrievableScheduler.Run] may succeed only once
    (ERROR__SCHEDULER_RERUN on retry)
  - Push before Run → ERROR__SCHEDULER_NOT_RUNNING
  - Push after Close / nest canceled → ERROR__SCHEDULER_CANCELED
  - Pending limit exceeded → ERROR__REACH_MAX_PENDING
  - Parent context cancel and manual Close both surface as
    ERROR__SCHEDULER_CANCELED (optionally wrapping the underlying cause)

Pending counts queue depth (decremented on Pop), not in-flight Job execution.

# Options

Scheduler: MaxPending, Parallel, FIFO/LIFO, Callback, ExitCallback,
CloseTimeout, WithoutDetached.

RetrievableScheduler: MaxPending, Parallel, FIFO/LIFO, CloseTimeout,
WithoutDetached (no Callback / ExitCallback).

By default, running jobs use context.WithoutCancel (detached from scheduler
cancel). Use WithoutDetached / WithoutRetrievableDetached to propagate cancel
into Job.Do. CloseTimeout bounds waiting for workers via nest, not Result
unblocking (Retrievable fails Results in BeforeClose).

Usage (Scheduler):

	s := schedx.NewScheduler(
		schedx.JobFunc[int](func(ctx context.Context, v int) error {
			return nil
		}),
		schedx.WithMaxPending[int](100),
		schedx.WithParallel[int](8),
	)
	if err := s.Run(ctx); err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	_ = s.Push(ctx, 42)

Usage (RetrievableScheduler):

	s := schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[int, int](func(ctx context.Context, in int) (int, error) {
			return in * 2, nil
		}),
		schedx.WithRetrievableMaxPending[int, int](100),
	)
	if err := s.Run(ctx); err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	ret, err := s.Push(ctx, 21)
	if err != nil {
		return err
	}
	out, err := ret.Result(ctx)

For structured concurrency without a job queue, use package nest directly.
*/
package schedx
