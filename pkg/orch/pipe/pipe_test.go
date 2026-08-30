package pipe_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/xoctopus/x/testx"

	"github.com/xoctopus/concx/pkg/orch/pipe"
)

func build(opts ...pipe.OptionFunc) pipe.Scheduler[int, int] {
	b := pipe.FromJob[int, int, int](
		"double",
		pipe.TransformFunc[int, int](func(_ context.Context, in int) (int, error) {
			if in < 0 {
				return 0, errors.New("negative")
			}
			return in * 2, nil
		}),
	).EndJob(
		"id",
		pipe.TransformFunc[int, int](func(_ context.Context, in int) (int, error) {
			return in, nil
		}),
	)
	return b.Build(opts...)
}

func TestPipePushResult(t *testing.T) {
	ctx := context.Background()
	p := build(pipe.WithMaxPending(4), pipe.WithParallel(2))

	_, err := p.Push(ctx, 1)
	Expect(t, err, IsCodeError(pipe.ERROR__PIPELINE_NOT_RUNNING))

	Expect(t, p.Run(ctx), Succeed())
	defer func() { _ = p.Close() }()

	ret, err := p.Push(ctx, 21)
	Expect(t, err, Succeed())
	out, err := ret.Result(ctx)
	Expect(t, err, Succeed())
	Expect(t, out, Equal(42))

	ret, err = p.Push(ctx, -1)
	Expect(t, err, Succeed())
	_, err = ret.Result(ctx)
	Expect(t, err, IsCodeError(pipe.ERROR__JOB_FAILED))
	Expect(t, err, ErrorContains("negative"))
}

func TestPipeCloseUnblocksResults(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})

	p := pipe.FromJob[string, string, string](
		"block",
		pipe.TransformFunc[string, string](func(c context.Context, in string) (string, error) {
			close(started)
			<-c.Done()
			return "", c.Err()
		}),
	).EndJob(
		"tail",
		pipe.TransformFunc[string, string](func(_ context.Context, in string) (string, error) {
			return in, nil
		}),
	).Build(
		pipe.WithMaxPending(4),
		pipe.WithParallel(1),
		pipe.WithShutdownTimeout(time.Second),
	)

	Expect(t, p.Run(ctx), Succeed())

	inFlight, err := p.Push(ctx, "in-flight")
	Expect(t, err, Succeed())
	<-started

	waiting, err := p.Push(ctx, "waiting")
	Expect(t, err, Succeed())

	Expect(t, p.Close(), Succeed())

	_, err = inFlight.Result(ctx)
	Expect(t, pipe.IsShutdown(err), Equal(true))
	_, err = waiting.Result(ctx)
	Expect(t, pipe.IsShutdown(err), Equal(true))

	_, err = p.Push(ctx, "after")
	Expect(t, err, IsCodeError(pipe.ERROR__PIPELINE_CANCELED))
}

func TestPipeMaxPending(t *testing.T) {
	ctx := context.Background()
	block := make(chan struct{})

	p := pipe.FromJob[int, int, int](
		"block",
		pipe.TransformFunc[int, int](func(c context.Context, in int) (int, error) {
			select {
			case <-c.Done():
				return 0, c.Err()
			case <-block:
				return in, nil
			}
		}),
	).EndJob(
		"id",
		pipe.TransformFunc[int, int](func(_ context.Context, in int) (int, error) {
			return in, nil
		}),
	).Build(
		pipe.WithMaxPending(1),
		pipe.WithParallel(1),
	)

	Expect(t, p.Run(ctx), Succeed())
	defer func() { _ = p.Close() }()

	ret, err := p.Push(ctx, 1)
	Expect(t, err, Succeed())

	// Worker busy in Do; another Push may sit in the admission queue (pending=1).
	time.Sleep(20 * time.Millisecond)
	ret2, err := p.Push(ctx, 2)
	Expect(t, err, Succeed())
	_, err = p.Push(ctx, 3)
	Expect(t, err, IsCodeError(pipe.ERROR__REACH_MAX_PENDING))

	close(block)
	_, err = ret.Result(ctx)
	Expect(t, err, Succeed())
	_, err = ret2.Result(ctx)
	Expect(t, err, Succeed())
}

func TestPipePanic(t *testing.T) {
	ctx := context.Background()
	p := pipe.FromJob[int, int, int](
		"panic",
		pipe.TransformFunc[int, int](func(_ context.Context, _ int) (int, error) {
			panic(errors.New("boom"))
		}),
	).EndJob(
		"id",
		pipe.TransformFunc[int, int](func(_ context.Context, in int) (int, error) {
			return in, nil
		}),
	).Build(pipe.WithMaxPending(1))

	Expect(t, p.Run(ctx), Succeed())
	defer func() { _ = p.Close() }()

	ret, err := p.Push(ctx, 1)
	Expect(t, err, Succeed())
	_, err = ret.Result(ctx)
	Expect(t, err, IsCodeError(pipe.ERROR__PIPELINE_JOB_PANICKED))
	Expect(t, err, ErrorContains("boom"))
}

func TestPipeParallel(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	seen := map[string]struct{}{}

	p := pipe.FromJob[string, string, string](
		"init",
		pipe.TransformFunc[string, string](func(_ context.Context, in string) (string, error) {
			return in, nil
		}),
	).Parallel[string](
		"fan",
		func(in string, outs ...any) string {
			return outs[0].(string) + "+" + outs[1].(string)
		},
		pipe.TransformFunc[string, any](func(_ context.Context, in string) (any, error) {
			mu.Lock()
			seen["a"] = struct{}{}
			mu.Unlock()
			return "a", nil
		}),
		pipe.TransformFunc[string, any](func(_ context.Context, in string) (any, error) {
			mu.Lock()
			seen["b"] = struct{}{}
			mu.Unlock()
			return "b", nil
		}),
	).EndJob(
		"done",
		pipe.TransformFunc[string, string](func(_ context.Context, in string) (string, error) {
			return in, nil
		}),
	).Build()

	Expect(t, p.Run(ctx), Succeed())
	defer func() { _ = p.Close() }()

	ret, err := p.Push(ctx, "x")
	Expect(t, err, Succeed())
	out, err := ret.Result(ctx)
	Expect(t, err, Succeed())
	Expect(t, out, Equal("a+b"))
	Expect(t, len(seen), Equal(2))
}
