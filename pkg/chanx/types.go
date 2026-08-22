package chanx

type Cancelable interface {
	CancelCause(err error)
}

type ValueNotifier[T any] interface {
	Send(x T)
}

type ValueObserver[T any] interface {
	Value() <-chan T
}

type Observable[T any] interface {
	Observe() Observer[T]
}

type Observer[T any] interface {
	ValueObserver[T]
	Cancelable

	Done() <-chan struct{}
	Err() error
}

type NotifiableObserver[T any] interface {
	Observer[T]
	ValueNotifier[T]
}

type Subscriber[T any] interface {
	ValueNotifier[T]
	Cancelable

	Done() <-chan struct{}
	Err() error
}

type ObserverFunc[T any] func() Observer[T]

func (f ObserverFunc[T]) Observe() Observer[T] {
	return f()
}
