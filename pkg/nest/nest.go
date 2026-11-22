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

// spawner is the only capability a child needs from its nearest ancestor.
type spawner interface {
	Spawn(func(context.Context)) error
}

type tCtxParent struct{}

// New creates a Nest whose lifetime is bound to ctx. If ctx was derived from
// another Nest's Children(), the new Nest registers itself with the nearest
// ancestor so that the ancestor's Cancel waits for it and the close signal
// flows down through ctx. WithDetached detaches it from both: the ancestor
// neither waits for it nor cancels it.
func New(ctx context.Context, appliers ...OptionApplier) Nest {
	x := &nest{
		done: make(chan struct{}),
	}

	for _, applier := range appliers {
		applier(&x.option)
	}

	parent, hasParent := ctx.Value(tCtxParent{}).(spawner)
	if x.detached {
		ctx = context.WithoutCancel(ctx)
	}
	x.inherited = ctx

	var dispatched context.Context
	dispatched, x.cancel = context.WithCancelCause(ctx)
	x.dispatched = context.WithValue(dispatched, tCtxParent{}, x)

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

	if hasParent && !x.detached {
		// If the parent is already closed, Spawn fails; the child is then
		// canceled by its own watcher because ctx is already done.
		_ = parent.Spawn(func(context.Context) { <-x.done })
	}

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
	select {
	case <-x.done:
		return x.err
	default:
		return nil
	}
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

// Cancel shuts the nest down and waits for its goroutines to exit.
// mu is held only to flip canceled, so Spawn calls from inside a running
// goroutine can always acquire it and observe the closed state. Waiting
// happens without mu held. Concurrent callers wait for the first one to finish.
func (x *nest) Cancel(cause error) {
	x.mu.Lock()
	if !x.canceled.CompareAndSwap(false, true) {
		x.mu.Unlock()
		<-x.done
		return
	}
	// canceled is set under mu, so no Spawn can call wg.Add after this point.
	x.mu.Unlock()

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

type option struct {
	shutdownTimeout time.Duration
	beforeCloseFunc func(context.Context)
	afterCloseFunc  func(error) error
	detached        bool
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

// WithDetached detaches the nest from its ancestor's lifetime: it is not
// registered with the ancestor, so the ancestor does not wait for it, and it
// is not canceled by the ancestor's ctx (context.WithoutCancel). It must be
// closed by its own Cancel.
func WithDetached() OptionApplier {
	return func(o *option) {
		o.detached = true
	}
}
