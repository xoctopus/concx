package pipe_test

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/xoctopus/x/slicex"

	"github.com/xoctopus/concx/pkg/orch/pipe"
)

// ExampleFromJob shows Build → Run → Push → Result → Close.
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
			sort.Strings(res)
			fmt.Println("summary:", res)
			return "finished", nil
		}),
	).Build(
		pipe.WithMaxPending(2),
		pipe.WithShutdownTimeout(time.Second),
	)

	_ = p.Run(ctx)
	defer func() { _ = p.Close() }()

	for _, name := range []string{"first", "second"} {
		ret, _ := p.Push(ctx, name)
		res, _ := ret.Result(ctx)
		fmt.Println(res, name)
	}

	// Unordered Output:
	// initializing: first
	// dag-a node: first
	// dag-b node: first
	// summary: [dag-a-result dag-b-result]
	// finished first
	// initializing: second
	// dag-a node: second
	// dag-b node: second
	// summary: [dag-a-result dag-b-result]
	// finished second
}
