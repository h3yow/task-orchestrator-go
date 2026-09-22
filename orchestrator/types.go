package orchestrator

import (
	"context"
	"time"
)

// Status is the state-machine status of a single task.
type Status string

const (
	StatusPending   Status = "pending"   // registered, not yet ready or running
	StatusReady     Status = "ready"     // all deps succeeded, eligible to run
	StatusRunning   Status = "running"   // an attempt is in flight
	StatusRetrying  Status = "retrying"  // attempt failed retryably, waiting for backoff
	StatusSucceeded Status = "succeeded" // terminal: exactly one successful effect
	StatusFailed    Status = "failed"    // terminal: retries exhausted / non-retryable error / timeout
	StatusSkipped   Status = "skipped"   // terminal: a dependency failed/skipped
)

// FailureReason categorizes why a task reached a terminal/attempt failure.
type FailureReason string

const (
	ReasonNone             FailureReason = ""
	ReasonNonRetryable     FailureReason = "non_retryable_error"
	ReasonRetriesExhausted FailureReason = "retries_exhausted"
	ReasonTimeout          FailureReason = "timeout"
	ReasonDependencyFailed FailureReason = "dependency_failed"
	ReasonInterrupted      FailureReason = "interrupted"
	ReasonRetryableError   FailureReason = "retryable_error"
)

// TaskFunc executes one task attempt. Returning nil marks success.
// A non-nil error is retryable unless wrapped with NonRetryable or
// implementing the PermanentError marker interface.
type TaskFunc func(ctx context.Context) error

// TaskDef declares a task and its dependency edges.
type TaskDef struct {
	ID         string
	Deps       []string
	Run        TaskFunc
	Retry      RetryPolicy
	Timeout    time.Duration // 0 => no per-attempt timeout
	MaxRuntime time.Duration // reserved
}

// RetryPolicy configures retry semantics for a task.
type RetryPolicy struct {
	MaxAttempts        int           // total attempts including the first; <=1 means no retry
	Backoff            time.Duration // wait between attempts; 0 => run immediately
	TimeoutIsRetryable bool          // whether an attempt timeout counts as retryable
}

// AttemptRecord captures the outcome of one task attempt.
type AttemptRecord struct {
	Attempt   int
	StartedAt time.Time
	EndedAt   time.Time
	Err       string
	Reason    FailureReason
	Timeout   bool
}

// TaskState is the persisted state-machine state of one task.
type TaskState struct {
	ID         string
	Status     Status
	Attempt    int
	Records    []AttemptRecord
	LastReason FailureReason
	UpdatedAt  time.Time
}

// Snapshot is the full persisted state of one run.
type Snapshot struct {
	RunID     string
	PlanHash  string
	CreatedAt time.Time
	UpdatedAt time.Time
	Tasks     map[string]*TaskState
}

// Logger is the minimal logging surface used by the engine.
type Logger interface {
	Printf(format string, args ...any)
}

// Options configures an Engine.
type Options struct {
	Store          Store
	Clock          Clock
	Logger         Logger
	MaxConcurrency int // <=0 means unlimited
}
