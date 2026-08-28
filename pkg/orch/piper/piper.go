package piper

type (
	Operator[I, O any] func(I) O
)

func Exec[Head, Tail any](
	v Head,
	op Operator[Head, Tail]) Tail {
	return op(v)
}

func Exec2[Head, A, Tail any](
	v Head,
	op1 Operator[Head, A],
	op2 Operator[A, Tail]) Tail {
	return Exec(op1(v), op2)
}

func Exec3[Head, A, B, Tail any](
	v Head,
	op1 Operator[Head, A],
	op2 Operator[A, B],
	op3 Operator[B, Tail]) Tail {
	return Exec2(op1(v), op2, op3)
}

func Exec4[Head, A, B, C, Tail any](
	v Head,
	op1 Operator[Head, A],
	op2 Operator[A, B],
	op3 Operator[B, C],
	op4 Operator[C, Tail]) Tail {
	return Exec3(op1(v), op2, op3, op4)
}

func Exec5[Head, A, B, C, D, Tail any](
	v Head,
	op1 Operator[Head, A],
	op2 Operator[A, B],
	op3 Operator[B, C],
	op4 Operator[C, D],
	op5 Operator[D, Tail]) Tail {
	return Exec4(op1(v), op2, op3, op4, op5)
}

func Exec6[Head, A, B, C, D, E, Tail any](
	v Head,
	op1 Operator[Head, A],
	op2 Operator[A, B],
	op3 Operator[B, C],
	op4 Operator[C, D],
	op5 Operator[D, E],
	op6 Operator[E, Tail]) Tail {
	return Exec5(op1(v), op2, op3, op4, op5, op6)
}

func Exec7[Head, A, B, C, D, E, F, Tail any](
	v Head,
	op1 Operator[Head, A],
	op2 Operator[A, B],
	op3 Operator[B, C],
	op4 Operator[C, D],
	op5 Operator[D, E],
	op6 Operator[E, F],
	op7 Operator[F, Tail]) Tail {
	return Exec6(op1(v), op2, op3, op4, op5, op6, op7)
}

func Exec8[Head, A, B, C, D, E, F, G, Tail any](
	v Head,
	op1 Operator[Head, A],
	op2 Operator[A, B],
	op3 Operator[B, C],
	op4 Operator[C, D],
	op5 Operator[D, E],
	op6 Operator[E, F],
	op7 Operator[F, G],
	op8 Operator[G, Tail]) Tail {
	return Exec7(op1(v), op2, op3, op4, op5, op6, op7, op8)
}
