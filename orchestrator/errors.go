package orchestrator

import "errors"

// Sentinel errors. Callers can use errors.Is / errors.As to distinguish
// failure causes before and during execution.
var (
	// Plan validation errors (rejected before any task runs):
	ErrDuplicateTask      = errors.New("duplicate task id")
	ErrMissingDependency  = errors.New("dependency points to a non-existent task")
	ErrCircularDependency = errors.New("circular dependency detected")

	// Trigger errors:
	ErrTaskNotFound     = errors.New("task not found")
	ErrNotReady         = errors.New("task is not eligible to run")
	ErrAlreadyRunning   = errors.New("task attempt is already in flight")
	ErrAlreadySucceeded = errors.New("task already succeeded")
	ErrTerminal         = errors.New("task is already in a terminal state")

	// Persistence / recovery errors:
	ErrInvalidState = errors.New("invalid or self-contradictory persisted state")
	ErrPlanMismatch = errors.New("persisted state belongs to a different plan")
	ErrStoreFault   = errors.New("state store failure")

	// Engine lifecycle:
	ErrEngineClosed = errors.New("engine is closed")
)
