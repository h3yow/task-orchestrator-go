package orchestrator

import "context"

// trigger is the API implementation behind Trigger. It hands the request to
// the single scheduler goroutine and waits for its verdict, so duplicate
// triggers can never race into two executions.
func (e *Engine) trigger(ctx context.Context, taskID string) error {
	if _, ok := e.defs[taskID]; !ok {
		return &ValidationError{Reason: ReasonInvalidTask, TaskID: taskID, Detail: "unknown task"}
	}
	if !e.runStarted.Load() {
		return ErrEngineNotRunning
	}

	reply := make(chan error, 1)
	ev := event{kind: evTrigger, taskID: taskID, reply: reply}
	select {
	case e.events <- ev:
	case <-e.stop:
		return ErrEngineNotRunning
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-reply:
		// nil: the task was dispatched; *ErrTriggerIgnored: duplicate / too late;
		// ErrDependencyFailed: deps already failed terminally.
		return err
	case <-e.stop:
		return ErrEngineNotRunning
	case <-ctx.Done():
		return ctx.Err()
	}
}
