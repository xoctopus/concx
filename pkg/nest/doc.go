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

For job queues and parallel scheduling, use package schedx instead.
*/
package nest
