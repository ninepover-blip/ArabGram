package loadharness

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

// A successfully decoded reply that violates a data contract is not an ordinary
// RPC rejection. Keep its cause for local diagnosis and a fixed public class.
type loadInvariantError struct {
	class string
	cause error
}

func (e *loadInvariantError) Error() string { return e.cause.Error() }
func (e *loadInvariantError) Unwrap() error { return e.cause }
func invariantError(class, format string, args ...any) error {
	return &loadInvariantError{class: class, cause: fmt.Errorf(format, args...)}
}
func invariantClass(err error) string {
	var v *loadInvariantError
	if errors.As(err, &v) {
		return v.class
	}
	return ""
}

type contextInvoker struct{ next tg.Invoker }

func (v contextInvoker) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return v.next.Invoke(ctx, in, out)
}
func workloadInvoker(record SessionRecord, base tg.Invoker, wrap func(SessionRecord, tg.Invoker) tg.Invoker) tg.Invoker {
	if wrap != nil {
		base = wrap(record, base)
	}
	return contextInvoker{next: base}
}

type deadlineInvoker struct {
	next    tg.Invoker
	timeout time.Duration
}

func (v deadlineInvoker) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	operation, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	return v.next.Invoke(operation, in, out)
}
