package pipe_test

import (
	"context"
	"fmt"

	"github.com/xoctopus/concx/pkg/orch/pipe"
	"github.com/xoctopus/concx/pkg/schedx"
)

// ExampleNew shows a linear pipe: single-job node → parallel multi-job node → single-job node.
func ExampleNew() {
	ctx := context.Background()

	p := pipe.New(ctx,
		pipe.NewNode(
			pipe.WithName[string]("validate"),
			pipe.WithJobs(schedx.JobFunc[string](func(_ context.Context, v string) error {
				fmt.Println("validate:", v)
				return nil
			})),
		),
		pipe.NewNode(
			pipe.WithName[string]("enrich"),
			pipe.WithJobs(
				schedx.JobFunc[string](func(_ context.Context, v string) error {
					fmt.Println("enrich-a:", v)
					return nil
				}),
				schedx.JobFunc[string](func(_ context.Context, v string) error {
					fmt.Println("enrich-b:", v)
					return nil
				}),
			),
		),
		pipe.NewNode(
			pipe.WithName[string]("persist"),
			pipe.WithJobs(schedx.JobFunc[string](func(_ context.Context, v string) error {
				fmt.Println("persist:", v)
				return nil
			})),
		),
	)
	defer func() { _ = p.Close() }()

	_ = p.Push(ctx, "item-1")
	fmt.Println("result:", <-p.Result())

	// Unordered output:
	// validate: item-1
	// enrich-a: item-1
	// enrich-b: item-1
	// persist: item-1
	// result: item-1
}
