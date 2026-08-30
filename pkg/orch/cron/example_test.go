package cron_test

import (
	"context"
	"fmt"
	"time"

	"github.com/xoctopus/concx/pkg/orch/cron"
	"github.com/xoctopus/concx/pkg/orch/pipe"
	"github.com/xoctopus/concx/pkg/schedx"
)

// ExampleNew demonstrates registering a recurring job.
func ExampleNew() {
	ctx := context.Background()

	c, _ := cron.New(ctx, "@every 50ms", schedx.JobFunc[time.Time](func(_ context.Context, _ time.Time) error {
		fmt.Println("periodic tick")
		return nil
	}), cron.WithName("periodic"), cron.WithShutdownTimeout(2*time.Second))
	defer func() { _ = c.Close() }()

	time.Sleep(60 * time.Millisecond)

	// Output:
	// periodic tick
}

// Example_pipeline demonstrates driving a pipe.Scheduler with a periodic cron trigger.
func Example_pipeline() {
	ctx := context.Background()

	p := pipe.FromJob[string, string, string](
		"fetch",
		pipe.TransformFunc[string, string](func(_ context.Context, v string) (string, error) {
			return v, nil
		}),
	).EndJob(
		"persist",
		pipe.TransformFunc[string, string](func(_ context.Context, v string) (string, error) {
			return v, nil
		}),
	).Build(
		pipe.WithMaxPending(4),
		pipe.WithParallel(2),
	)

	_ = p.Run(ctx)
	defer func() { _ = p.Close() }()

	c := cron.NewWithSchedule(
		ctx,
		cron.MustSpec("@every 10s"),
		schedx.JobFunc[time.Time](func(ctx context.Context, t time.Time) error {
			ret, err := p.Push(ctx, t.Format(time.RFC3339))
			if err != nil {
				return err
			}
			_, err = ret.Result(ctx)
			return err
		}),
		cron.WithOverlapSkip(),
	)
	defer func() { _ = c.Close() }()
}
