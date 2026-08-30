package pipe

import (
	"context"
	"sync/atomic"

	"github.com/xoctopus/x/codex"

	"github.com/xoctopus/concx/pkg/chanx"
	"github.com/xoctopus/concx/pkg/nest"
)

type TransformJob[In, Out any] interface {
	Do(ctx context.Context, in In) (Out, error)
}

type TransformFunc[In, Out any] func(context.Context, In) (Out, error)

func (f TransformFunc[In, Out]) Do(ctx context.Context, in In) (Out, error) {
	return f(ctx, in)
}

type UniversalTransformJob[In any] = TransformJob[In, any]

type Summary[I, O any] func(I, ...any) O

func execute[I, O any](ctx context.Context, in I, j TransformJob[I, O]) (out O, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case error:
				err = codex.Wrap(ERROR__PIPELINE_JOB_PANICKED, x)
			default:
				err = codex.Errorf(ERROR__PIPELINE_JOB_PANICKED, "caused by: %v", x)
			}
		}
	}()

	if out, err = j.Do(ctx, in); err != nil {
		err = codex.Wrap(ERROR__JOB_FAILED, err)
	}
	return
}

func summary[I, O any](f Summary[I, O], in I, res ...any) (out O, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case error:
				err = codex.Wrap(ERROR__PIPELINE_SUMMARY_PANICKED, x)
			default:
				err = codex.Errorf(ERROR__PIPELINE_SUMMARY_PANICKED, "caused by: %v", x)
			}
		}
	}()

	return f(in, res...), nil
}

type Node interface {
	Name() string
	execute(context.Context) error
}

type SimpleNode[Head, Tail, I, O any] struct {
	name     string
	job      TransformJob[I, O]
	observer chanx.ValueObserver[*flight[Head, Tail, I]]
	notifier chanx.ValueNotifier[*flight[Head, Tail, O]]
}

func (n *SimpleNode[Head, Tail, I, O]) Name() string {
	return n.name
}

func (n *SimpleNode[Head, Tail, I, O]) execute(ctx context.Context) (err error) {
	var (
		in  *flight[Head, Tail, I]
		ok  bool
		out O
	)

	defer func() {
		if err != nil && in != nil {
			in.t.failed(err)
		}
	}()

	select {
	case <-ctx.Done():
		return codex.Wrap(ERROR__PIPELINE_CANCELED, ctx.Err())
	case in, ok = <-n.observer.Value():
		if !ok {
			return codex.New(ERROR__PIPELINE_CANCELED)
		}
	}

	out, err = execute(ctx, in.v, n.job)
	if err != nil {
		return err
	}

	if n.notifier != nil {
		n.notifier.Send(&flight[Head, Tail, O]{t: in.t, v: out})
	}

	return nil
}

type ParallelNode[Head, Tail, I, O any] struct {
	name     string
	jobs     []UniversalTransformJob[I]
	summary  func(in I, outs ...any) O
	observer chanx.ValueObserver[*flight[Head, Tail, I]]
	notifier chanx.ValueNotifier[*flight[Head, Tail, O]]
}

func (n *ParallelNode[Head, Tail, I, O]) Name() string {
	return n.name
}

func (n *ParallelNode[Head, Tail, I, O]) execute(ctx context.Context) (err error) {
	var (
		in *flight[Head, Tail, I]
		ok bool
	)

	defer func() {
		if err != nil && in != nil {
			in.t.failed(err)
		}
	}()

	select {
	case <-ctx.Done():
		return codex.Wrap(ERROR__PIPELINE_CANCELED, ctx.Err())
	case in, ok = <-n.observer.Value():
		if !ok {
			return codex.New(ERROR__PIPELINE_CANCELED)
		}
	}

	var (
		g    = nest.New(ctx)
		fail atomic.Value
		left atomic.Int64
		res  = make([]any, len(n.jobs))
	)

	left.Store(int64(len(n.jobs)))

	for idx, j := range n.jobs {
		if err := g.Spawn(func(ctx context.Context) {
			out, err := execute(ctx, in.v, j)
			if err != nil {
				fail.CompareAndSwap(nil, err)
				go g.Cancel(err)
				return
			}
			res[idx] = out
			if left.Add(-1) == 0 {
				go g.Cancel(nil)
			}
		}); err != nil {
			fail.CompareAndSwap(nil, err)
			go g.Cancel(err)
			break
		}
	}

	<-g.Done()

	if err, ok := fail.Load().(error); ok && err != nil {
		return err
	}

	if n.notifier != nil {
		out := *(new(O))
		if n.summary != nil {
			if out, err = summary(n.summary, in.v, res...); err != nil {
				return err
			}
		}
		n.notifier.Send(&flight[Head, Tail, O]{t: in.t, v: out})
	}

	return nil
}
