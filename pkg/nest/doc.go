/*
Package nest provides a guarded, derivable goroutine lifecycle boundary.

A Nest tracks spawned workers via sync.WaitGroup, separates Parent (inherited)
from Children (dispatched) context, and supports graceful Cancel with optional
shutdown timeout.

Usage:

	n := nest.New(ctx, nest.WithShutdownTimeout(5*time.Second))

	n.Spawn(func(ctx context.Context) {
	    doWork(ctx)
	})

	n.Cancel(nil)
	<-n.Done()

Parent and child nests:

A nest created from another nest's Children() context is a child of that nest.
By default, the child registers with its nearest ancestor, so the ancestor's
Cancel waits until the child is done, and the close signal flows down through
the context. Pass WithDetached to opt out: the child is then not canceled by
its ancestor (it uses context.WithoutCancel) and the ancestor does not wait
for it. A detached nest must be closed by its own Cancel.

Lifecycle rules:

  - Spawn returns an error once Cancel has started; a worker spawned from
    inside a running worker or a hook gets that error instead of blocking.
  - Cancel holds the internal lock only to flip the closed state. Waiting for
    workers and running hooks happen without the lock.
  - Concurrent Cancel calls wait until the first one finishes.
  - Err is non-blocking: it returns nil until Done is closed.
  - WithShutdownTimeout bounds the wait for workers; on timeout Cancel returns
    with ERROR__NEST_CLOSE_TIMEOUT and the remaining workers keep running.

For job queues and parallel scheduling, use package schedx instead.
*/
package nest
