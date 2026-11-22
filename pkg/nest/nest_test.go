package nest_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xoctopus/x/misc/must"
	. "github.com/xoctopus/x/testx"

	. "github.com/xoctopus/concx/pkg/nest"
)

type MockWorker func(ctx context.Context, cost time.Duration, id int)

func NewAndSpawn(ctx context.Context, worker MockWorker, cost, shutdown time.Duration) Nest {
	options := []OptionApplier{
		WithBeforeCloseFunc(func(_ context.Context) {
			fmt.Println("Before done")
		}),
		WithAfterCloseFunc(func(err error) error {
			fmt.Printf("After: %v\n", err)
			return nil
		}),
	}
	if shutdown > 0 {
		options = append(options, WithShutdownTimeout(shutdown*unit))
	}
	n := New(ctx, options...)

	for i := 1; i <= 3; i++ {
		workerID := i
		must.NoError(
			n.Spawn(func(ctx context.Context) {
				worker(ctx, cost*unit, workerID)
			}),
		)
	}
	return n
}

var (
	unit = time.Millisecond * 100

	worker1 = func(ctx context.Context, cost time.Duration, wid int) {
		select {
		case <-time.After(cost):
			fmt.Printf("Worker %d finished task\n", wid)
		case <-ctx.Done():
			fmt.Printf("Worker %d canceled\n", wid)
		}
	}

	worker2 = func(ctx context.Context, cost time.Duration, wid int) {
		select {
		case <-time.After(cost):
			fmt.Printf("Worker %d finished task\n", wid)
		}
	}
)

func ExampleNest() {
	outside, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	ctx, cancel2 := context.WithCancel(outside)
	defer cancel2()

	fmt.Println("==> worker was scheduled before nest.Cancel")
	n := NewAndSpawn(ctx, worker1, 1, 1)
	time.Sleep(3 * unit)
	n.Cancel(nil)
	fmt.Printf("Closed: %v\n\n", n.Err())

	fmt.Println("==> worker was not scheduled before nest.Cancel")
	n = NewAndSpawn(ctx, worker1, 2, 1)
	time.Sleep(1 * unit)
	n.Cancel(nil)
	fmt.Printf("Closed: %v\n\n", n.Err())

	fmt.Println("==> without shutdown timeout")
	n = NewAndSpawn(ctx, worker2, 5, 0)
	time.Sleep(8 * unit)
	n.Cancel(nil)
	fmt.Printf("Closed: %v\n\n", n.Err())

	fmt.Println("==> nest close timeout")
	n = NewAndSpawn(ctx, worker2, 5, 1)
	n.Cancel(errors.New("cause"))
	fmt.Printf("Closed: %v\n\n", n.Err())
	time.Sleep(6 * unit)

	fmt.Println("==> shutdown triggered by outside nest.Cancel")
	n = NewAndSpawn(ctx, worker2, 5, 1)
	cancel(errors.New("outside"))
	<-n.Done()
	fmt.Printf("Closed: %v\n\n", n.Err())
	time.Sleep(6 * unit)
	_ = 1

	// Unordered Output:
	// ==> worker was scheduled before nest.Cancel
	// Worker 1 finished task
	// Worker 2 finished task
	// Worker 3 finished task
	// Before done
	// After: <nil>
	// Closed: <nil>
	//
	// ==> worker was not scheduled before nest.Cancel
	// Before done
	// Worker 1 canceled
	// Worker 2 canceled
	// Worker 3 canceled
	// After: <nil>
	// Closed: <nil>
	//
	// ==> without shutdown timeout
	// Worker 1 finished task
	// Worker 2 finished task
	// Worker 3 finished task
	// Before done
	// After: <nil>
	// Closed: <nil>
	//
	// ==> nest close timeout
	// Before done
	// After: [NEST::ERROR:2] NEST_CLOSE_TIMEOUT
	// cause
	// Closed: [NEST::ERROR:2] NEST_CLOSE_TIMEOUT
	// cause
	//
	// Worker 1 finished task
	// Worker 2 finished task
	// Worker 3 finished task
	// ==> shutdown triggered by outside nest.Cancel
	// Before done
	// After: [NEST::ERROR:2] NEST_CLOSE_TIMEOUT
	// outside
	// Closed: [NEST::ERROR:2] NEST_CLOSE_TIMEOUT
	// outside
	//
	// Worker 1 finished task
	// Worker 2 finished task
	// Worker 3 finished task
}

func TestNewNest(t *testing.T) {
	var (
		ctx    = context.Background()
		cancel context.CancelFunc
	)
	ctx = context.WithValue(ctx, "key", 100)
	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	n := New(ctx)
	Expect(t, n.Parent(), Equal(ctx))
	Expect(t, n.Children(), NotEqual(ctx))

	Expect(t, n.Value("key"), Equal(ctx.Value("key")))
	Expect(t, n.Value("non"), Equal(ctx.Value("non")))

	ts1, ok1 := n.Deadline()
	ts2, ok2 := ctx.Deadline()
	Expect(t, ts1, Equal(ts2))
	Expect(t, ok1, Equal(ok2))

	Expect(t, n.Canceled(), BeFalse())
	n.Cancel(nil)
	<-n.Done()
	Expect(t, n.Canceled(), BeTrue())
	Expect(t, n.Spawn(func(ctx context.Context) {}), IsCodeError(ERROR__NEST_CLOSED))
}

// WaitOrFail fails the test instead of hanging when fn does not return in time.
func WaitOrFail(t *testing.T, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: deadlock, did not finish within 5s", name)
	}
}

func TestNestSpawnFromRunningWorkerDuringCancel(t *testing.T) {
	n := New(context.Background())

	var spawnErr = make(chan error, 1)
	started := make(chan struct{})
	must.NoError(n.Spawn(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		// a goroutine already inside the nest spawns again while Cancel is waiting for it
		spawnErr <- n.Spawn(func(context.Context) {})
	}))
	<-started

	WaitOrFail(t, "Cancel with nested Spawn", func() {
		n.Cancel(nil)
	})
	Expect(t, <-spawnErr, IsCodeError(ERROR__NEST_CLOSED))
}

func TestNestSpawnFromBeforeCloseFunc(t *testing.T) {
	var n Nest
	hookErr := make(chan error, 1)
	n = New(context.Background(), WithBeforeCloseFunc(func(context.Context) {
		// Spawn from the before-close hook runs inside Cancel and must not deadlock on mu
		hookErr <- n.Spawn(func(context.Context) {})
	}))
	WaitOrFail(t, "Cancel with Spawn in beforeCloseFunc", func() {
		n.Cancel(nil)
	})
	Expect(t, <-hookErr, IsCodeError(ERROR__NEST_CLOSED))
}

func TestNestErrDoesNotBlockBeforeCancel(t *testing.T) {
	n := New(context.Background())

	WaitOrFail(t, "Err before Cancel", func() {
		if err := n.Err(); err != nil {
			t.Errorf("Err before Cancel = %v, want nil", err)
		}
	})

	n.Cancel(errors.New("cause"))
	WaitOrFail(t, "Err after Cancel", func() {
		if err := n.Err(); err == nil {
			t.Errorf("Err after Cancel = nil, want non-nil")
		}
	})
}

func TestNestConcurrentCancel(t *testing.T) {
	n := New(context.Background())
	must.NoError(n.Spawn(func(ctx context.Context) {
		<-ctx.Done()
		time.Sleep(unit)
	}))

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			n.Cancel(nil)
		})
	}
	WaitOrFail(t, "concurrent Cancel", wg.Wait)

	select {
	case <-n.Done():
	default:
		t.Fatal("Done not closed after Cancel returned")
	}
}

func TestNestParentWaitsForChild(t *testing.T) {
	parent := New(context.Background())
	child := New(parent.Children())

	var finished atomic.Bool
	must.NoError(child.Spawn(func(ctx context.Context) {
		<-ctx.Done()
		time.Sleep(unit)
		finished.Store(true)
	}))

	WaitOrFail(t, "parent Cancel", func() { parent.Cancel(nil) })
	Expect(t, finished.Load(), BeTrue())
	Expect(t, child.Canceled(), BeTrue())
}

func TestNestGrandchildChain(t *testing.T) {
	root := New(context.Background())
	mid := New(root.Children())
	leaf := New(mid.Children())

	var finished atomic.Bool
	must.NoError(leaf.Spawn(func(ctx context.Context) {
		<-ctx.Done()
		time.Sleep(unit)
		finished.Store(true)
	}))

	WaitOrFail(t, "root Cancel", func() { root.Cancel(nil) })
	Expect(t, finished.Load(), BeTrue())
	Expect(t, leaf.Canceled(), BeTrue())
}

func TestNestChildOfClosedParent(t *testing.T) {
	parent := New(context.Background())
	parent.Cancel(nil)

	child := New(parent.Children())
	WaitOrFail(t, "child of closed parent", func() { <-child.Done() })
	Expect(t, child.Canceled(), BeTrue())
}

func TestNestWithDetachedIsNotWaitedNorCanceled(t *testing.T) {
	parent := New(context.Background())
	release := make(chan struct{})

	child := New(parent.Children(), WithDetached())
	must.NoError(child.Spawn(func(context.Context) { <-release }))

	WaitOrFail(t, "parent Cancel", func() { parent.Cancel(nil) })
	if child.Canceled() {
		t.Fatal("detached child was canceled by parent")
	}
	select {
	case <-child.Done():
		t.Fatal("detached child was waited by parent")
	default:
	}

	close(release)
	WaitOrFail(t, "detached child own Cancel", func() { child.Cancel(nil) })
	Expect(t, child.Canceled(), BeTrue())
}

func TestNestParentTimeoutWithStuckChild(t *testing.T) {
	parent := New(context.Background(), WithShutdownTimeout(unit))
	child := New(parent.Children())
	release := make(chan struct{})
	must.NoError(child.Spawn(func(context.Context) { <-release }))

	WaitOrFail(t, "parent Cancel with stuck child", func() { parent.Cancel(nil) })
	if err := parent.Err(); err == nil || !strings.Contains(err.Error(), "NEST_CLOSE_TIMEOUT") {
		t.Fatalf("parent Err = %v, want NEST_CLOSE_TIMEOUT", err)
	}

	close(release)
	WaitOrFail(t, "stuck child done", func() { <-child.Done() })
}

func TestNestNewChildConcurrentWithParentCancel(t *testing.T) {
	for range 50 {
		parent := New(context.Background())

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			parent.Cancel(nil)
		}()
		go func() {
			defer wg.Done()
			child := New(parent.Children())
			<-child.Done()
		}()
		WaitOrFail(t, "concurrent child creation and parent Cancel", wg.Wait)
	}
}
