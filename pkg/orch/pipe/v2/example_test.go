package pipe_test

import (
	"context"
	"fmt"
	"time"

	"github.com/xoctopus/x/slicex"

	pipe "github.com/xoctopus/concx/pkg/orch/pipe/v2"
)

// ExampleFromJob shows how to build and run a pipeline with v2 DSL and Retriever.
func ExampleFromJob() {
	ctx := context.Background()

	p := pipe.FromJob[string, string, string](
		"initializing",
		pipe.TransformFunc[string, string](func(_ context.Context, in string) (string, error) {
			fmt.Println("initializing:", in)
			return in, nil
		}),
	).Parallel[[]string](
		"dag",
		func(in string, outs ...any) []string {
			return slicex.Mapping(outs, func(v any) string { return v.(string) })
		},
		pipe.TransformFunc[string, any](func(_ context.Context, in string) (any, error) {
			fmt.Println("dag-a node:", in)
			return "dag-a-result", nil
		}),
		pipe.TransformFunc[string, any](func(_ context.Context, in string) (any, error) {
			fmt.Println("dag-b node:", in)
			return "dag-b-result", nil
		}),
	).EndJob(
		"summary",
		pipe.TransformFunc[[]string, string](func(_ context.Context, res []string) (string, error) {
			fmt.Println("summary:", res)
			return "finished", nil
		}),
	).Build()

	go func() {
		_ = p.Run(ctx)
	}()
	time.Sleep(time.Millisecond * 50)

	defer func() { _ = p.Close() }()

	ret, _ := p.Push(ctx, "origin")
	res, _ := ret.Result(ctx)
	fmt.Println("result:", res)

	// Unordered Output:
	// initializing: origin
	// dag-a node: origin
	// dag-b node: origin
	// summary: [dag-a-result dag-b-result]
	// result: finished
}
