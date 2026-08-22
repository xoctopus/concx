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
