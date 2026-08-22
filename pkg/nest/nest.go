package nest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xoctopus/x/codex"
)

type Nest interface {
	context.Context

	Parent() context.Context
	Children() context.Context
	Spawn(func(context.Context)) error
	Cancel(error)
	Canceled() bool
}

func New(ctx context.Context, appliers ...OptionApplier) Nest {
	x := &nest{
		inherited: ctx,
		done:      make(chan struct{}),
	}

	for _, applier := range appliers {
		applier(&x.option)
	}

	x.dispatched, x.cancel = context.WithCancelCause(ctx)

	go func() {
		var cause error
		select {
		case <-x.inherited.Done():
			cause = context.Cause(x.inherited)
		case <-x.dispatched.Done():
			cause = context.Cause(x.dispatched)
		}
		x.Cancel(cause)
	}()

	return x
}

type nest struct {
	option

	inherited  context.Context
	cancel     context.CancelCauseFunc
	dispatched context.Context

	count    atomic.Int64
	mu       sync.Mutex
	wg       sync.WaitGroup
	canceled atomic.Bool
	err      error
	done     chan struct{}
}

func (x *nest) Parent() context.Context {
	return x.inherited
}

func (x *nest) Children() context.Context {
	return x.dispatched
}

func (x *nest) Deadline() (time.Time, bool) {
	return x.dispatched.Deadline()
}

func (x *nest) Done() <-chan struct{} {
	return x.done
}

func (x *nest) Err() error {
	<-x.Done()
	return x.err
}

func (x *nest) Value(key any) any {
	return x.dispatched.Value(key)
}

func (x *nest) Spawn(f func(ctx context.Context)) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.canceled.Load() {
		x.wg.Go(func() {
			f(x.dispatched)
		})
		return nil
	}
	return codex.New(ERROR__NEST_CLOSED)
}

func (x *nest) Canceled() bool {
	return x.canceled.Load()
}

func (x *nest) Cancel(cause error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.canceled.CompareAndSwap(false, true) {
		ctx, cancel := context.WithCancel(context.Background())
		if x.shutdownTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, x.shutdownTimeout)
		}
		var err error
		defer func() {
			cancel()
			x.err = errors.Join(err, cause)
			if x.afterCloseFunc != nil {
				x.err = errors.Join(x.err, x.afterCloseFunc(x.err))
			}
			close(x.done)
		}()

		if x.beforeCloseFunc != nil {
			x.beforeCloseFunc(ctx)
		}

		x.cancel(cause)

		if x.shutdownTimeout > 0 {
			sig := make(chan struct{})

			go func() {
				x.wg.Wait()
				close(sig)
			}()

			select {
			case <-sig:
				return
			case <-time.After(x.shutdownTimeout):
				err = codex.New(ERROR__NEST_CLOSE_TIMEOUT)
				return
			}
		}
		x.wg.Wait()
	}
}

type option struct {
	shutdownTimeout time.Duration
	beforeCloseFunc func(context.Context)
	afterCloseFunc  func(error) error
}

type OptionApplier func(*option)

func WithShutdownTimeout(timeout time.Duration) OptionApplier {
	return func(o *option) {
		o.shutdownTimeout = timeout
	}
}

// WithBeforeCloseFunc registers a hook that executes before Nest.Cancel
// process begins. The provided context is canceled either when all goroutines
// have finished or once the shutdownTimeout is reached if configurated.
// This is typically used to signal or wake up suspended goroutines (e.g., via sync.Cond).
// Note: The implementation of this function must decide whether to operate
// synchronously or asynchronously.
func WithBeforeCloseFunc(beforeCloseFunc func(context.Context)) OptionApplier {
	return func(o *option) {
		o.beforeCloseFunc = beforeCloseFunc
	}
}

// WithAfterCloseFunc registers a post-close hook.
// ensures afterCloseFunc is invoked after all goroutines exited, providing
// synchronous waiting for Nest.Cancel.
// The err parameter passed to indicates why the shutdown was triggered (eg: context.Cause).
func WithAfterCloseFunc(afterCloseFunc func(error) error) OptionApplier {
	return func(o *option) {
		o.afterCloseFunc = afterCloseFunc
	}
}
