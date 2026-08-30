/*
Package piper is a lightweight typed operator chain: compose [Operator] /
[OperatorEx] left-to-right without schedulers, channels, or [pipe]-style
lifecycle.

Unlike [github.com/xoctopus/concx/pkg/orch/pipe], piper is synchronous and
inline. Use [Exec] / [Exec2] … [Exec8] for pure transforms (I → O), or
[ExecEx] / [ExecEx2] … [ExecEx8] when each step takes a [context.Context] and
may return an error. [ExecEx] variants stop at the first error and return the
zero Tail.

	out := piper.Exec3(in, parse, validate, format)
	out, err := piper.ExecEx2(ctx, in, load, save)

[Operator] is I → O. [OperatorEx] is (ctx, I) → (O, error). Intermediate type
parameters (A, B, …) are inferred from the operator signatures.
*/
package piper
