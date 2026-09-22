package orchestrator

import (
	"errors"
	"time"
)

// errPermanent is matched via errors.Is for wrapped permanent failures.
var errPermanent = errors.New("permanent failure")

// permanentError marks a task error as non-retryable.
type permanentError struct {
	err error
}

func (e *permanentError) Error() string {
	if e.err == nil {
		return "permanent failure"
	}
	return "permanent failure: " + e.err.Error()
}

func (e *permanentError) Unwrap() error { return e.err }

func (e *permanentError) Is(target error) bool { return target == errPermanent }

// PermanentError is an optional interface a task error may implement to
// declare itself non-retryable without using NonRetryable().
type PermanentError interface {
	error
	IsPermanent() bool
}

// NonRetryable wraps err so the engine treats the failure as permanent:
// the task moves straight to StatusFailed without further attempts.
// NonRetryable(nil) returns nil.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsNonRetryable reports whether err is marked permanent, either via
// NonRetryable wrapping or by implementing the PermanentError interface.
func IsNonRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errPermanent) {
		return true
	}
	var pe PermanentError
	if errors.As(err, &pe) {
		return pe.IsPermanent()
	}
	return false
}

// backoffDelay returns the wait before the next attempt. The policy uses a
// fixed backoff (zero means the retry runs immediately, which is what the
// deterministic unit tests rely on so no real wall-clock waiting is needed).
func backoffDelay(policy RetryPolicy) time.Duration {
	if policy.Backoff < 0 {
		return 0
	}
	return policy.Backoff
}

// maxAttempts normalizes the configured attempt count; at least one
// attempt always happens.
func maxAttempts(policy RetryPolicy) int {
	if policy.MaxAttempts < 1 {
		return 1
	}
	return policy.MaxAttempts
}
