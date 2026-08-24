package pipe_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/xoctopus/x/testx"

	"github.com/xoctopus/concx/pkg/orch/pipe"
	"github.com/xoctopus/concx/pkg/schedx"
)

func TestPipe(t *testing.T) {
	ctx := context.Background()

	p := pipe.New(ctx,
		pipe.NewNode(
			pipe.WithJobs[int](schedx.JobFunc[int](func(_ context.Context, v int) error {
				return nil
			})),
			pipe.WithName[int]("first"),
		),
		pipe.NewNode(
			pipe.WithJobs[int](schedx.JobFunc[int](func(_ context.Context, v int) error {
				return nil
			})),
			pipe.WithName[int]("second"),
		),
	)
	defer func() { _ = p.Close() }()

	Expect(t, p.Push(ctx, 42), Succeed())

	select {
	case v := <-p.Result():
		Expect(t, v, Equal(42))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}
}

func TestPipeMultiple(t *testing.T) {
	ctx := context.Background()

	p := pipe.New(ctx,
		pipe.NewNode(
			pipe.WithJobs[int](schedx.JobFunc[int](func(_ context.Context, v int) error {
				return nil
			})),
		),
		pipe.NewNode(
			pipe.WithJobs[int](schedx.JobFunc[int](func(_ context.Context, v int) error {
				return nil
			})),
		),
	)
	defer func() { _ = p.Close() }()

	got := make(map[int]struct{})
	for _, v := range []int{1, 2} {
		Expect(t, p.Push(ctx, v), Succeed())
		select {
		case out := <-p.Result():
			got[out] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for result")
		}
	}
	Expect(t, len(got), Equal(2))
}

func TestPipeJobErrorDoesNotForward(t *testing.T) {
	ctx := context.Background()

	p := pipe.New(ctx,
		pipe.NewNode(
			pipe.WithJobs[int](schedx.JobFunc[int](func(_ context.Context, v int) error {
				if v == 2 {
					return context.Canceled
				}
				return nil
			})),
		),
		pipe.NewNode(
			pipe.WithJobs[int](schedx.JobFunc[int](func(_ context.Context, v int) error {
				return nil
			})),
		),
	)
	defer func() { _ = p.Close() }()

	Expect(t, p.Push(ctx, 1), Succeed())
	select {
	case v := <-p.Result():
		Expect(t, v, Equal(1))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for single result")
	}

	Expect(t, p.Push(ctx, 2), Succeed())
	select {
	case _, ok := <-p.Result():
		Expect(t, ok, Equal(false))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestPipeNodeParallelJobs(t *testing.T) {
	ctx := context.Background()
	var (
		mu    sync.Mutex
		steps []int
	)
	record := func(v int) {
		mu.Lock()
		steps = append(steps, v)
		mu.Unlock()
	}

	var started atomic.Int32
	gate := make(chan struct{})

	p := pipe.New(ctx,
		pipe.NewNode(
			pipe.WithJobs[int](
				schedx.JobFunc[int](func(_ context.Context, v int) error {
					started.Add(1)
					<-gate
					record(v + 1)
					return nil
				}),
				schedx.JobFunc[int](func(_ context.Context, v int) error {
					started.Add(1)
					<-gate
					record(v + 2)
					return nil
				}),
			),
			pipe.WithJobs[int](schedx.JobFunc[int](func(_ context.Context, v int) error {
				started.Add(1)
				<-gate
				record(v + 3)
				return nil
			})),
		),
	)
	defer func() { _ = p.Close() }()

	Expect(t, p.Push(ctx, 10), Succeed())

	deadline := time.After(2 * time.Second)
	for started.Load() < 3 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for parallel jobs to start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(gate)

	select {
	case v := <-p.Result():
		Expect(t, v, Equal(10))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	mu.Lock()
	defer mu.Unlock()
	Expect(t, len(steps), Equal(3))
	seen := map[int]bool{}
	for _, s := range steps {
		seen[s] = true
	}
	Expect(t, seen[11] && seen[12] && seen[13], Equal(true))
}

func TestPipeNodeFailFastDoesNotForward(t *testing.T) {
	ctx := context.Background()
	var forwarded atomic.Bool

	p := pipe.New(ctx,
		pipe.NewNode(
			pipe.WithJobs[int](
				schedx.JobFunc[int](func(c context.Context, _ int) error {
					<-c.Done()
					return c.Err()
				}),
				schedx.JobFunc[int](func(_ context.Context, _ int) error {
					return context.Canceled
				}),
			),
		),
		pipe.NewNode(
			pipe.WithJobs[int](schedx.JobFunc[int](func(_ context.Context, _ int) error {
				forwarded.Store(true)
				return nil
			})),
		),
	)
	defer func() { _ = p.Close() }()

	Expect(t, p.Push(ctx, 1), Succeed())

	select {
	case <-p.Result():
		t.Fatal("failed node should not forward")
	case <-time.After(200 * time.Millisecond):
	}
	Expect(t, forwarded.Load(), Equal(false))
}

func TestPipeClose(t *testing.T) {
	ctx := context.Background()
	p := pipe.New(ctx,
		pipe.NewNode(
			pipe.WithJobs[int](schedx.JobFunc[int](func(_ context.Context, v int) error {
				return nil
			})),
		),
	)
	Expect(t, p.Close(), Succeed())
	Expect(t, p.Push(ctx, 1), IsCodeError(schedx.ERROR__SCHEDULER_CANCELED))
	Expect(t, p.Close(), Succeed())

	select {
	case <-p.Done():
	default:
		t.Fatal("Done should be closed after Close")
	}
	_, ok := <-p.Result()
	Expect(t, ok, Equal(false))
}
