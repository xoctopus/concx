package schedx_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xoctopus/x/misc/must"
	. "github.com/xoctopus/x/testx"

	"github.com/xoctopus/concx/pkg/schedx"
)

func TestRetrievableScheduler(t *testing.T) {
	ctx := context.Background()

	s := schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[int, int](func(_ context.Context, in int) (int, error) {
			if in < 0 {
				return 0, errors.New("negative")
			}
			return in * 2, nil
		}),
		schedx.WithRetrievableMaxPending(2),
		schedx.WithoutRetrievableDetached(),
	)

	_, err := s.Push(ctx, 1)
	Expect(t, err, IsCodeError(schedx.ERROR__SCHEDULER_NOT_RUNNING))

	Expect(t, s.Run(ctx), Succeed())
	defer func() { _ = s.Close() }()

	ret, err := s.Push(ctx, 21)
	Expect(t, err, Succeed())
	out, err := ret.Result(ctx)
	Expect(t, err, Succeed())
	Expect(t, out, Equal(42))

	ret, err = s.Push(ctx, -1)
	Expect(t, err, Succeed())
	_, err = ret.Result(ctx)
	Expect(t, err, ErrorContains("negative"))
}

func TestRetrievableSchedulerMaxPendingAndRerun(t *testing.T) {
	ctx := context.Background()
	block := make(chan struct{})

	s := schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[int, int](func(c context.Context, in int) (int, error) {
			select {
			case <-c.Done():
				return 0, c.Err()
			case <-block:
				return in, nil
			}
		}),
		schedx.WithRetrievableMaxPending(1),
		schedx.WithRetrievableParallel(1),
		schedx.WithoutRetrievableDetached(),
	)

	Expect(t, s.Run(ctx), Succeed())
	Expect(t, s.Run(ctx), IsCodeError(schedx.ERROR__SCHEDULER_RERUN))

	ret, err := s.Push(ctx, 1)
	Expect(t, err, Succeed())

	// Pending is queue depth only. After Pop, pending--, so while the sole
	// worker is busy, another Push can enter the queue (pending=1). A third
	// Push then hits REACH_MAX_PENDING.
	time.Sleep(20 * time.Millisecond)
	ret2, err := s.Push(ctx, 2)
	Expect(t, err, Succeed())
	_, err = s.Push(ctx, 3)
	Expect(t, err, IsCodeError(schedx.ERROR__REACH_MAX_PENDING))

	close(block)
	_, err = ret.Result(ctx)
	Expect(t, err, Succeed())
	_, err = ret2.Result(ctx)
	Expect(t, err, Succeed())

	Expect(t, s.Close(), Succeed())
	_, err = s.Push(ctx, 4)
	Expect(t, err, IsCodeError(schedx.ERROR__SCHEDULER_CANCELED))
}

func TestRetrievableSchedulerCloseUnblocksResults(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})

	s := schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[string, string](func(c context.Context, in string) (string, error) {
			close(started)
			<-c.Done()
			return "", c.Err()
		}),
		schedx.WithRetrievableMaxPending(4),
		schedx.WithoutRetrievableDetached(),
		schedx.WithRetrievableCloseTimeout(time.Second),
	)

	Expect(t, s.Run(ctx), Succeed())

	// One task is already in Do; another waits in the queue. Close must fail
	// both Results immediately (SCHEDULER_CANCELED).
	inFlight, err := s.Push(ctx, "in-flight")
	Expect(t, err, Succeed())
	<-started

	waiting, err := s.Push(ctx, "waiting")
	Expect(t, err, Succeed())

	Expect(t, s.Close(), Succeed())

	_, err = inFlight.Result(ctx)
	Expect(t, err, IsCodeError(schedx.ERROR__SCHEDULER_CANCELED))
	_, err = waiting.Result(ctx)
	Expect(t, err, IsCodeError(schedx.ERROR__SCHEDULER_CANCELED))
}

func TestRetrievableSchedulerPanic(t *testing.T) {
	ctx := context.Background()
	s := schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[int, int](func(_ context.Context, _ int) (int, error) {
			panic(errors.New("boom"))
		}),
		schedx.WithRetrievableMaxPending(1),
	)
	Expect(t, s.Run(ctx), Succeed())
	defer func() { _ = s.Close() }()

	ret, err := s.Push(ctx, 1)
	Expect(t, err, Succeed())
	_, err = ret.Result(ctx)
	Expect(t, err, IsCodeError(schedx.ERROR__SCHEDULER_JOB_PANICKED))
	Expect(t, err, ErrorContains("boom"))
}

// ExampleNewRetrievableScheduler shows Run → Push → Result → Close.
func ExampleNewRetrievableScheduler() {
	ctx := context.Background()

	s := schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[int, string](func(_ context.Context, in int) (string, error) {
			return fmt.Sprintf("n=%d", in*2), nil
		}),
		schedx.WithRetrievableMaxPending(8),
		schedx.WithRetrievableParallel(2),
	)

	_ = s.Run(ctx)
	defer func() {
		_ = s.Close()
	}()

	ret, _ := s.Push(ctx, 21)

	out, _ := ret.Result(ctx)
	fmt.Println(out)

	// Output:
	// n=42
}

func BenchmarkRetrievablePushConcurrent(b *testing.B) {
	ctx := context.Background()

	s := schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[int, int](func(context.Context, int) (int, error) {
			return 0, nil
		}),
		schedx.WithoutRetrievablePendingLimitation(),
		schedx.WithRetrievableParallel(8),
		schedx.WithRetrievableCloseTimeout(0),
	)

	must.NoError(s.Run(ctx))
	defer func() { _ = s.Close() }()

	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = s.Push(ctx, 1)
		}
	})
}

func BenchRetrievable(b *testing.B, options ...schedx.RetrievableSchedulerApplier) {
	ctx := context.Background()

	s := schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[int, int](func(context.Context, int) (int, error) {
			return 0, nil
		}),
		options...,
	)
	defer func() { _ = s.Close() }()

	must.NoError(s.Run(ctx))

	b.ResetTimer()
	for b.Loop() {
		_, _ = s.Push(ctx, 1)
	}

	for s.Pending() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	b.StopTimer()
}

func BenchmarkRetrievable(b *testing.B) {
	timeout := time.Duration(0)
	b.Run("LIFO_x16", func(b *testing.B) {
		BenchRetrievable(
			b,
			schedx.WithoutRetrievablePendingLimitation(),
			schedx.WithRetrievableParallel(16),
			schedx.WithRetrievableLifoScheduleMode(),
			schedx.WithRetrievableCloseTimeout(timeout),
		)
	})

	b.Run("FIFO_x16", func(b *testing.B) {
		BenchRetrievable(
			b,
			schedx.WithoutRetrievablePendingLimitation(),
			schedx.WithRetrievableParallel(16),
			schedx.WithRetrievableFifoScheduleMode(),
			schedx.WithRetrievableCloseTimeout(timeout),
		)
	})

	for parallel := 100; parallel <= schedx.MaxParallel; parallel *= 10 {
		b.Run("FIFO_x"+strconv.Itoa(parallel), func(b *testing.B) {
			BenchRetrievable(
				b,
				schedx.WithoutRetrievablePendingLimitation(),
				schedx.WithRetrievableParallel(parallel+1),
				schedx.WithRetrievableFifoScheduleMode(),
				schedx.WithRetrievableCloseTimeout(timeout),
			)
		})
		runtime.GC()
		b.Run("LIFO_x"+strconv.Itoa(parallel), func(b *testing.B) {
			BenchRetrievable(
				b,
				schedx.WithoutRetrievablePendingLimitation(),
				schedx.WithRetrievableParallel(parallel+1),
				schedx.WithRetrievableLifoScheduleMode(),
				schedx.WithRetrievableCloseTimeout(timeout),
			)
		})
		runtime.GC()
	}
}

// BenchmarkRetrievablePushResult: Push is one flow; each Result is awaited
// in its own goroutine (wg.Go), not on the Push path.
func BenchmarkRetrievablePushResult(b *testing.B) {
	ctx := context.Background()

	s := schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[int, int](func(_ context.Context, in int) (int, error) {
			return in, nil
		}),
		schedx.WithoutRetrievablePendingLimitation(),
		schedx.WithRetrievableParallel(8),
		schedx.WithRetrievableCloseTimeout(0),
	)

	must.NoError(s.Run(ctx))
	defer func() { _ = s.Close() }()

	var wg sync.WaitGroup

	b.ResetTimer()
	for b.Loop() {
		ret, err := s.Push(ctx, 1)
		if err != nil {
			b.Fatal(err)
		}
		wg.Go(func() {
			if _, err := ret.Result(ctx); err != nil {
				b.Error(err)
			}
		})
	}
	wg.Wait()
	b.StopTimer()
}
