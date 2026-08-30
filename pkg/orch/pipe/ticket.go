package pipe

import (
	"context"
	"sync"

	"github.com/xoctopus/x/codex"

	"github.com/xoctopus/concx/pkg/schedx"
)

// Result is the completion handle returned by [Scheduler.Push].
type Result[R any] = schedx.Result[R]

// ticket synchronizes one in-flight pipeline item between stages and the
// Retrievable Job.Do that waits for Tail (or failure).
type ticket[Tail any] struct {
	once sync.Once
	done chan struct{}

	out Tail
	err error
}

func (t *ticket[Tail]) finish(out Tail, err error) {
	t.once.Do(func() {
		t.out, t.err = out, err
		close(t.done)
	})
}

func (t *ticket[Tail]) failed(err error) {
	t.finish(*new(Tail), err)
}

func (t *ticket[Tail]) wait(ctx context.Context, closed <-chan struct{}) (Tail, error) {
	select {
	case <-t.done:
		return t.out, t.err
	default:
	}

	select {
	case <-t.done:
		return t.out, t.err
	case <-ctx.Done():
		return *new(Tail), codex.Wrap(ERROR__PIPELINE_CANCELED, ctx.Err())
	case <-closed:
		select {
		case <-t.done:
			return t.out, t.err
		default:
			return *new(Tail), codex.New(ERROR__PIPELINE_CANCELED)
		}
	}
}

// flight carries one stage value and the ticket that owns the pipeline Result.
type flight[Head, Tail, T any] struct {
	t *ticket[Tail]
	v T
}
