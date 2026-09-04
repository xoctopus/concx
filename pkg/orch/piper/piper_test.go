package piper_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	. "github.com/xoctopus/x/testx"

	"github.com/xoctopus/concx/pkg/orch/piper"
)

func TestExec(t *testing.T) {
	out := piper.Exec(21, piper.Operator[int, int](func(v int) int {
		return v * 2
	}))
	Expect(t, out, Equal(42))
}

func TestExec3(t *testing.T) {
	out := piper.Exec3(
		"10",
		piper.Operator[string, int](func(v string) int {
			n, _ := strconv.Atoi(v)
			return n
		}),
		piper.Operator[int, int](func(v int) int { return v + 1 }),
		piper.Operator[int, string](strconv.Itoa),
	)
	Expect(t, out, Equal("11"))
}

func TestExecEx(t *testing.T) {
	ctx := context.Background()

	out, err := piper.ExecEx(ctx, 5, piper.OperatorEx[int, int](func(_ context.Context, v int) (int, error) {
		return v * 3, nil
	}))
	Expect(t, err, Succeed())
	Expect(t, out, Equal(15))
}

func TestExecEx2Success(t *testing.T) {
	ctx := context.Background()

	out, err := piper.ExecEx2(
		ctx,
		"7",
		piper.OperatorEx[string, int](func(_ context.Context, v string) (int, error) {
			return strconv.Atoi(v)
		}),
		piper.OperatorEx[int, int](func(_ context.Context, v int) (int, error) {
			return v + 2, nil
		}),
	)
	Expect(t, err, Succeed())
	Expect(t, out, Equal(9))
}

func TestExecEx2ErrorOnFirstStep(t *testing.T) {
	ctx := context.Background()
	errFirst := errors.New("first failed")

	var secondCalled bool
	out, err := piper.ExecEx2(
		ctx,
		1,
		piper.OperatorEx[int, int](func(_ context.Context, _ int) (int, error) {
			return 0, errFirst
		}),
		piper.OperatorEx[int, string](func(_ context.Context, _ int) (string, error) {
			secondCalled = true
			return "never", nil
		}),
	)
	Expect(t, err, Equal(errFirst))
	Expect(t, out, Equal(""))
	Expect(t, secondCalled, Equal(false))
}

func TestExecEx3ErrorOnMiddleStep(t *testing.T) {
	ctx := context.Background()
	errMiddle := errors.New("middle failed")

	var thirdCalled bool
	out, err := piper.ExecEx3(
		ctx,
		1,
		piper.OperatorEx[int, int](func(_ context.Context, v int) (int, error) {
			return v + 1, nil
		}),
		piper.OperatorEx[int, int](func(_ context.Context, _ int) (int, error) {
			return 0, errMiddle
		}),
		piper.OperatorEx[int, string](func(_ context.Context, _ int) (string, error) {
			thirdCalled = true
			return "never", nil
		}),
	)
	Expect(t, err, Equal(errMiddle))
	Expect(t, out, Equal(""))
	Expect(t, thirdCalled, Equal(false))
}
