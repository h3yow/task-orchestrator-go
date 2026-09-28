package orchestrator

import "fmt"

// ValidationReason identifies why a task definition or DAG was rejected
// before any execution started.
type ValidationReason string

const (
	ReasonCycle            ValidationReason = "cycle_detected"
	ReasonUnknownDep       ValidationReason = "unknown_dependency"
	ReasonDuplicateID      ValidationReason = "duplicate_task_id"
	ReasonInvalidTask      ValidationReason = "invalid_task"
	ReasonInvalidState     ValidationReason = "invalid_persisted_state"
	ReasonStoreUnavailable ValidationReason = "store_unavailable"
	// ReasonPlanMismatch marks a snapshot that is internally intact but was
	// produced under different task definitions. It deliberately differs from
	// ReasonInvalidState (self-contradictory / corrupt / rolled-back state) so
	// callers can tell "the plan changed" apart from "the state is damaged".
	ReasonPlanMismatch ValidationReason = "plan_mismatch"
)

// ValidationError is returned before execution starts for any rejected
// definition or persisted-state problem. Reason lets callers distinguish the
// exact failure cause.
type ValidationError struct {
	Reason ValidationReason
	TaskID string
	Detail string
}

func (e *ValidationError) Error() string {
	if e.TaskID != "" {
		return fmt.Sprintf("validation failed (%s): task=%q: %s", e.Reason, e.TaskID, e.Detail)
	}
	return fmt.Sprintf("validation failed (%s): %s", e.Reason, e.Detail)
}

// ErrEngineNotRunning is returned when an API is used after the engine was
// stopped without being started, or after it already terminated.
var ErrEngineNotRunning = fmt.Errorf("orchestrator: engine is not running")

// ErrGracefulShutdown is returned by Run when Shutdown completed a drain: the
// scheduler stopped after dispatching nothing new and persisting every in-flight
// task in a stable state. It is distinct from context cancellation, which leaves
// running tasks open and is reconciled as "interrupted" on the next start.
var ErrGracefulShutdown = fmt.Errorf("orchestrator: graceful shutdown completed")

// ErrTriggerIgnored marks an explicit Trigger call whose task is already
// past its initial scheduling point (running / waiting-retry / terminal).
type ErrTriggerIgnored struct{ TaskID string }

func (e *ErrTriggerIgnored) Error() string {
	return fmt.Sprintf("orchestrator: task %q is not in a triggerable state", e.TaskID)
}
