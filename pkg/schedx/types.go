package schedx

import (
	"context"
)

type Job[T any] interface {
	Do(context.Context, T) error
}

type JobFunc[T any] func(context.Context, T) error

func (f JobFunc[T]) Do(ctx context.Context, v T) error {
	return f(ctx, v)
}

type Scheduler[T any] interface {
	Push(context.Context, T) error
	Run(context.Context) error
	Pending() int
	Close() error
}

type Tasks[T any] interface {
	Len() int
	Push(T)
	Pop() (T, bool)
	Range(func(T) bool)
	Clear()
}

type ScheduleMode int

const (
	FIFO ScheduleMode = iota
	LIFO
)

type RetrievableJob[In, Out any] interface {
	Do(context.Context, In) (Out, error)
}

type RetrievableJobFunc[In, Out any] func(context.Context, In) (Out, error)

func (f RetrievableJobFunc[In, Out]) Do(ctx context.Context, in In) (Out, error) {
	return f(ctx, in)
}

type Result[Out any] interface {
	Result(context.Context) (Out, error)
}

type RetrievableScheduler[In, Out any] interface {
	Push(context.Context, In) (Result[Out], error)
	Run(context.Context) error
	Pending() int
	Close() error
}
