package cron_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/xoctopus/x/testx"

	"github.com/xoctopus/concx/pkg/orch/cron"
	"github.com/xoctopus/concx/pkg/schedx"
)

func TestCronBasic(t *testing.T) {
	ctx := context.Background()

	var count atomic.Int32
	c, err := cron.New(ctx, "@every 30ms", schedx.JobFunc[time.Time](func(_ context.Context, _ time.Time) error {
		count.Add(1)
		return nil
	}), cron.WithName("counter"))

	Expect(t, err, Succeed())
	defer func() { _ = c.Close() }()

	time.Sleep(120 * time.Millisecond)
	Expect(t, count.Load() >= 2, Equal(true))
}

func TestCronWithSchedule(t *testing.T) {
	ctx := context.Background()

	var count atomic.Int32
	c := cron.NewWithSchedule(ctx, cron.Every(30*time.Millisecond), schedx.JobFunc[time.Time](func(_ context.Context, _ time.Time) error {
		count.Add(1)
		return nil
	}))
	defer func() { _ = c.Close() }()

	time.Sleep(100 * time.Millisecond)
	Expect(t, count.Load() >= 2, Equal(true))
}

func TestCronOverlapSkip(t *testing.T) {
	ctx := context.Background()

	var active atomic.Int32
	var maxActive atomic.Int32
	var totalRuns atomic.Int32

	c, err := cron.New(ctx, "@every 30ms", schedx.JobFunc[time.Time](func(_ context.Context, _ time.Time) error {
		cur := active.Add(1)
		totalRuns.Add(1)
		for {
			old := maxActive.Load()
			if cur <= old || maxActive.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(90 * time.Millisecond)
		active.Add(-1)
		return nil
	}), cron.WithOverlapSkip())

	Expect(t, err, Succeed())
	defer func() { _ = c.Close() }()

	time.Sleep(200 * time.Millisecond)
	Expect(t, maxActive.Load(), Equal(int32(1)))
	Expect(t, totalRuns.Load() <= 3, Equal(true))
}

func TestCronOverlapParallel(t *testing.T) {
	ctx := context.Background()

	var active atomic.Int32
	var maxActive atomic.Int32

	c, err := cron.New(ctx, "@every 30ms", schedx.JobFunc[time.Time](func(_ context.Context, _ time.Time) error {
		cur := active.Add(1)
		for {
			old := maxActive.Load()
			if cur <= old || maxActive.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		active.Add(-1)
		return nil
	}), cron.WithParallel(3))

	Expect(t, err, Succeed())
	defer func() { _ = c.Close() }()

	time.Sleep(150 * time.Millisecond)
	Expect(t, maxActive.Load() >= 2, Equal(true))
}

func TestCronOverlapQueue(t *testing.T) {
	ctx := context.Background()

	var count atomic.Int32
	c, err := cron.New(ctx, "@every 20ms", schedx.JobFunc[time.Time](func(_ context.Context, _ time.Time) error {
		time.Sleep(50 * time.Millisecond)
		count.Add(1)
		return nil
	}), cron.WithOverlapQueue(10))

	Expect(t, err, Succeed())
	defer func() { _ = c.Close() }()

	time.Sleep(150 * time.Millisecond)
	Expect(t, count.Load() >= 2, Equal(true))
}

func TestCronCallbackAndPanic(t *testing.T) {
	ctx := context.Background()

	var cbErr error
	var wg sync.WaitGroup
	wg.Add(1)

	c, err := cron.New(ctx, "@every 30ms", schedx.JobFunc[time.Time](func(_ context.Context, _ time.Time) error {
		panic("job boom")
	}), cron.WithCallback(func(_ time.Time, err error) {
		cbErr = err
		wg.Done()
	}))

	Expect(t, err, Succeed())
	defer func() { _ = c.Close() }()

	wg.Wait()
	Expect(t, cbErr != nil, Equal(true))
}

func TestCronInvalidSpec(t *testing.T) {
	ctx := context.Background()

	_, err := cron.New(ctx, "invalid cron spec string", schedx.JobFunc[time.Time](func(_ context.Context, _ time.Time) error {
		return nil
	}))
	Expect(t, err != nil, Equal(true))
}

func TestCronClose(t *testing.T) {
	ctx := context.Background()

	c, err := cron.New(ctx, "@every 20ms", schedx.JobFunc[time.Time](func(_ context.Context, _ time.Time) error {
		return nil
	}), cron.WithShutdownTimeout(500*time.Millisecond))

	Expect(t, err, Succeed())
	Expect(t, c.Close(), Succeed())

	select {
	case <-c.Done():
	default:
		t.Fatal("Done should be closed after Close")
	}
}
