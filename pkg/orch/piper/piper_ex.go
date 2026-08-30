package piper

import "context"

type OperatorEx[I, O any] func(context.Context, I) (O, error)

func ExecEx[Head, Tail any](
	ctx context.Context,
	v Head,
	op OperatorEx[Head, Tail]) (Tail, error) {
	return op(ctx, v)
}

func ExecEx2[Head, A, Tail any](
	ctx context.Context,
	v Head,
	op1 OperatorEx[Head, A],
	op2 OperatorEx[A, Tail],
) (x Tail, err error) {
	o, err := op1(ctx, v)
	if err != nil {
		return x, err
	}
	return ExecEx(ctx, o, op2)
}

func ExecEx3[Head, A, B, Tail any](
	ctx context.Context,
	v Head,
	op1 OperatorEx[Head, A],
	op2 OperatorEx[A, B],
	op3 OperatorEx[B, Tail],
) (x Tail, err error) {
	o, err := op1(ctx, v)
	if err != nil {
		return x, err
	}
	return ExecEx2(ctx, o, op2, op3)
}

func ExecEx4[Head, A, B, C, Tail any](
	ctx context.Context,
	v Head,
	op1 OperatorEx[Head, A],
	op2 OperatorEx[A, B],
	op3 OperatorEx[B, C],
	op4 OperatorEx[C, Tail],
) (x Tail, err error) {
	o, err := op1(ctx, v)
	if err != nil {
		return x, err
	}
	return ExecEx3(ctx, o, op2, op3, op4)
}

func ExecEx5[Head, A, B, C, D, Tail any](
	ctx context.Context,
	v Head,
	op1 OperatorEx[Head, A],
	op2 OperatorEx[A, B],
	op3 OperatorEx[B, C],
	op4 OperatorEx[C, D],
	op5 OperatorEx[D, Tail],
) (x Tail, err error) {
	o, err := op1(ctx, v)
	if err != nil {
		return x, err
	}
	return ExecEx4(ctx, o, op2, op3, op4, op5)
}

func ExecEx6[Head, A, B, C, D, E, Tail any](
	ctx context.Context,
	v Head,
	op1 OperatorEx[Head, A],
	op2 OperatorEx[A, B],
	op3 OperatorEx[B, C],
	op4 OperatorEx[C, D],
	op5 OperatorEx[D, E],
	op6 OperatorEx[E, Tail],
) (x Tail, err error) {
	o, err := op1(ctx, v)
	if err != nil {
		return x, err
	}
	return ExecEx5(ctx, o, op2, op3, op4, op5, op6)
}

func ExecEx7[Head, A, B, C, D, E, F, Tail any](
	ctx context.Context,
	v Head,
	op1 OperatorEx[Head, A],
	op2 OperatorEx[A, B],
	op3 OperatorEx[B, C],
	op4 OperatorEx[C, D],
	op5 OperatorEx[D, E],
	op6 OperatorEx[E, F],
	op7 OperatorEx[F, Tail],
) (x Tail, err error) {
	o, err := op1(ctx, v)
	if err != nil {
		return x, err
	}
	return ExecEx6(ctx, o, op2, op3, op4, op5, op6, op7)
}

func ExecEx8[Head, A, B, C, D, E, F, G, Tail any](
	ctx context.Context,
	v Head,
	op1 OperatorEx[Head, A],
	op2 OperatorEx[A, B],
	op3 OperatorEx[B, C],
	op4 OperatorEx[C, D],
	op5 OperatorEx[D, E],
	op6 OperatorEx[E, F],
	op7 OperatorEx[F, G],
	op8 OperatorEx[G, Tail],
) (x Tail, err error) {
	o, err := op1(ctx, v)
	if err != nil {
		return x, err
	}
	return ExecEx7(ctx, o, op2, op3, op4, op5, op6, op7, op8)
}
