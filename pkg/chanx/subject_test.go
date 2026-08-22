package chanx_test

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/xoctopus/x/iterx"

	"github.com/xoctopus/concx/pkg/chanx"
)

func ExampleSubject() {
	// Create a Subject
	subject := &chanx.Subject[int]{}

	// Create two observers
	obs1 := subject.Observe()
	obs2 := subject.Observe()

	var (
		wg      sync.WaitGroup
		results = make(chan string, 4)
	)

	consuming := func(name string, observer chanx.Observer[int]) func() {
		return func() {
			for v := range observer.Value() {
				results <- fmt.Sprintf("%s received: %d", name, v)
			}
		}
	}

	// Consumer 1
	wg.Go(consuming("obs1", obs1))
	// Consumer 2
	wg.Go(consuming("obs2", obs2))

	// Producer broadcasts data via the Subject
	subject.Send(1)
	time.Sleep(10 * time.Millisecond)
	subject.Send(2)
	time.Sleep(10 * time.Millisecond)

	// Close the Subject, which cascades the cancellation to all subscribers
	subject.CancelCause(nil)
	// Wait for all consumers to finish
	wg.Wait()
	close(results)

	for _, s := range slices.Sorted(iterx.Recv(results)) {
		fmt.Println(s)
	}

	// Verify the error state of the Subject and its Observers
	fmt.Println("Subject Error:", subject.Err())
	fmt.Println("Obs1 Error:", obs1.Err())
	fmt.Println("Obs2 Error:", obs2.Err())

	// Output:
	// obs1 received: 1
	// obs1 received: 2
	// obs2 received: 1
	// obs2 received: 2
	// Subject Error: completed
	// Obs1 Error: completed
	// Obs2 Error: completed
}
