package chanx_test

import (
	"fmt"
	"sync"
	"time"

	"github.com/xoctopus/concx/pkg/chanx"
)

func ExampleNotifiableObserver() {
	// Create a NotifiableObserver
	obs := chanx.NewNotifiableObserver[int]()

	var wg sync.WaitGroup

	// Start a producer goroutine
	wg.Go(func() {
		obs.Send(1)
		obs.Send(2)
		obs.Send(3)
		time.Sleep(10 * time.Millisecond)
		// Cancel after sending all data. A nil error will be converted to ErrCompleted.
		obs.CancelCause(nil)
	})

	// Consume data in the main goroutine until the channel is closed
	for v := range obs.Value() {
		fmt.Println("Received:", v)
	}

	wg.Wait()

	// Check the final error state
	fmt.Println("Error:", obs.Err())

	// Output:
	// Received: 1
	// Received: 2
	// Received: 3
	// Error: completed
}
