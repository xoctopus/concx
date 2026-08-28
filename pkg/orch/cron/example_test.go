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

// Example_pipeline demonstrates driving a pipe.Pipeline with a periodic cron trigger.
func Example_pipeline() {
	ctx := context.Background()

	// 1. Build a multi-stage pipeline.
	p := pipe.New(ctx,
		pipe.NewNode(
			pipe.WithName[string]("fetch"),
			pipe.WithJobs(schedx.JobFunc[string](func(_ context.Context, v string) error {
				return nil
			})),
		),
		pipe.NewNode(
			pipe.WithName[string]("persist"),
			pipe.WithJobs(schedx.JobFunc[string](func(_ context.Context, v string) error {
				return nil
			})),
		),
	)
	defer func() { _ = p.Close() }()

	// 2. Drive the pipeline every 10 seconds.
	c, _ := cron.New(ctx, "@every 10s", schedx.JobFunc[time.Time](func(ctx context.Context, t time.Time) error {
		return p.Push(ctx, t.Format(time.RFC3339))
	}), cron.WithOverlapSkip())
	defer func() { _ = c.Close() }()
}
