package orchestrator

import (
	"context"
	"time"
)

// Status is the lifecycle state of a task.
type Status string

const (
	StatusPending   Status = "pending"    // never started; waiting for dependencies / trigger
	StatusRunning   Status = "running"    // an attempt is in flight
	StatusRetryWait Status = "retry_wait" // attempt failed, waiting for the retry backoff
	StatusSucceeded Status = "succeeded"  // terminal: exactly one effective success
	StatusFailed    Status = "failed"     // terminal: retries exhausted, timeout or deps failed
)

func (s Status) terminal() bool { return s == StatusSucceeded || s == StatusFailed }

// AttemptClass distinguishes ordinary task failures from timeout and
// interruption failures, so that retry-vs-terminal decisions are explainable.
type AttemptClass string

const (
	AttemptSucceeded   AttemptClass = "succeeded"   // attempt completed successfully
	AttemptFailed      AttemptClass = "failed"      // Executor returned a failure
	AttemptTimeout     AttemptClass = "timeout"     // attempt exceeded the task timeout
	AttemptInterrupted AttemptClass = "interrupted" // engine restarted while attempt in flight
	AttemptSkippedDeps AttemptClass = "deps_failed" // synthesized when a dependency failed
)

// Attempt records one execution attempt of a task. Attempts are append-only
// audit evidence: they are never rewritten or deleted.
type Attempt struct {
	Seq       int          `json:"seq"`
	StartedAt time.Time    `json:"started_at"`
	EndedAt   time.Time    `json:"ended_at"`
	Class     AttemptClass `json:"class"`
	// Retryable records the Executor's verdict for failed attempts.
	Retryable bool   `json:"retryable,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// RetryPolicy controls retry semantics for a task.
type RetryPolicy struct {
	// MaxAttempts is the total attempt budget including the first attempt.
	// Values < 1 are normalized to 1.
	MaxAttempts int `json:"max_attempts"`
	// InitialBackoff is the delay before attempt #2. A Clock-based timer is
	// used, so tests never wait real time.
	InitialBackoff time.Duration `json:"initial_backoff"`
	// MaxBackoff caps the (linearly growing) backoff.
	MaxBackoff time.Duration `json:"max_backoff"`
}

// Backoff returns the delay before the given next attempt number
// (1-based; Backoff(2) is the delay between attempt 1 and 2).
// The backoff grows linearly from InitialBackoff and is capped at MaxBackoff.
func (p RetryPolicy) Backoff(nextAttempt int) time.Duration {
	if nextAttempt <= 1 || p.InitialBackoff <= 0 {
		return 0
	}
	d := p.InitialBackoff * time.Duration(nextAttempt-1)
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	return d
}

// normalized fills defaults: MaxAttempts<1 becomes 1, a missing MaxBackoff
// defaults to InitialBackoff (so the cap never silently shrinks the delay).
func (p RetryPolicy) normalized() RetryPolicy {
	n := p
	if n.MaxAttempts < 1 {
		n.MaxAttempts = 1
	}
	if n.MaxBackoff == 0 {
		n.MaxBackoff = n.InitialBackoff
	}
	if n.MaxBackoff < n.InitialBackoff {
		n.MaxBackoff = n.InitialBackoff
	}
	return n
}

// TaskDef is the static definition of a task.
type TaskDef struct {
	ID      string
	Deps    []string
	Timeout time.Duration // 0 = no per-attempt timeout
	Retry   RetryPolicy
}

// Result is what an Executor reports for one attempt.
type Result struct {
	OK        bool
	Retryable bool
	Reason    string
}

// Executor runs a single attempt of a task. Implementations must be
// idempotent-safe with respect to context cancellation: when ctx is done they
// must return promptly. Because an engine crash cannot be distinguished from
// a slow worker, at-least-once delivery applies — successful side effects
// after a timeout/replay are guarded by the engine contract that only one
// attempt can commit a success.
type Executor interface {
	Execute(ctx context.Context, taskID string) Result
}

// TaskState is the persisted state of one task.
type TaskState struct {
	Status    Status    `json:"status"`
	Attempts  []Attempt `json:"attempts"`
	NextRunAt time.Time `json:"next_run_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (ts *TaskState) attemptCount() int { return len(ts.Attempts) }

// Snapshot is the whole persisted state of an engine instance.
type Snapshot struct {
	// Version is strictly incremented on every committed change; loaded
	// snapshots with a non-monotonic version are rejected.
	Version int `json:"version"`
	// PlanDigest is the SHA-256 fingerprint of the exact task plan this state
	// belongs to (deps, timeout, retry policy). A restart resumes only when the
	// supplied definitions digest to the same value. Snapshots written before
	// this field existed are checked structurally instead.
	PlanDigest string                `json:"plan_digest,omitempty"`
	Tasks      map[string]*TaskState `json:"tasks"`
}
