package orchestrator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Engine drives scheduling, concurrent execution, retry/timeout handling
// and crash recovery of a validated Plan.
type Engine struct {
	plan  *Plan
	store Store
	clock Clock
	log   Logger
	sem   chan struct{} // nil => unlimited concurrency

	mu       sync.Mutex
	states   map[string]*TaskState
	created  time.Time
	queued   map[string]bool // ready tasks whose executor goroutine is starting
	wg       sync.WaitGroup
	wakeCh   chan struct{}
	closed   bool
	cancelFn context.CancelFunc
	bgCtx    context.Context
	runID    string
}

// TaskError summarizes one task that did not succeed after a Run.
type TaskError struct {
	ID        string
	Status    Status
	Reason    FailureReason
	Attempts  int
	LastError string
}

// RunError is returned by Run when at least one task failed or was skipped.
type RunError struct {
	Tasks []TaskError
}

func (e *RunError) Error() string {
	parts := make([]string, 0, len(e.Tasks))
	for _, t := range e.Tasks {
		parts = append(parts, fmt.Sprintf("%s=%s(%s)", t.ID, t.Status, t.Reason))
	}
	sort.Strings(parts)
	return "run did not finish successfully: " + strings.Join(parts, ", ")
}

// NewEngine creates an engine, loads persisted state (if any) and applies
// crash recovery. A fresh run starts with every task persisted as pending
// before Run is called.
func NewEngine(plan *Plan, opts Options) (*Engine, error) {
	if plan == nil {
		return nil, fmt.Errorf("%w: nil plan", ErrInvalidState)
	}
	e := &Engine{
		plan:   plan,
		store:  opts.Store,
		clock:  opts.Clock,
		log:    opts.Logger,
		states: make(map[string]*TaskState),
		queued: make(map[string]bool),
		wakeCh: make(chan struct{}, 1),
		runID:  plan.RunID(),
	}
	if e.store == nil {
		e.store = NewMemoryStore()
	}
	if e.clock == nil {
		e.clock = NewRealClock()
	}
	if e.log == nil {
		e.log = discardLogger{}
	}
	if opts.MaxConcurrency > 0 {
		e.sem = make(chan struct{}, opts.MaxConcurrency)
	}

	snap, err := e.store.Load(context.Background(), plan.RunID())
	if err != nil {
		return nil, err
	}
	if snap == nil {
		snap = e.freshSnapshot()
		if err := e.store.Save(context.Background(), snap); err != nil {
			return nil, err
		}
		e.log.Printf("[scheduler] run=%q decision=persist-initial all tasks pending (no previous state)", plan.RunID())
	} else {
		e.log.Printf("[scheduler] run=%q decision=load-persisted-state existing snapshot found", plan.RunID())
	}
	if err := validateSnapshot(snap, plan); err != nil {
		return nil, err
	}
	for id, st := range snap.Tasks {
		e.states[id] = st
	}
	e.created = snap.CreatedAt

	if err := e.recover(); err != nil {
		return nil, err
	}
	e.bgCtx, e.cancelFn = context.WithCancel(context.Background())
	return e, nil
}

func (e *Engine) freshSnapshot() *Snapshot {
	now := e.clock.Now()
	snap := &Snapshot{
		RunID:     e.plan.RunID(),
		PlanHash:  e.plan.Hash(),
		CreatedAt: now,
		UpdatedAt: now,
		Tasks:     make(map[string]*TaskState, len(e.plan.IDs())),
	}
	for _, id := range e.plan.IDs() {
		snap.Tasks[id] = &TaskState{ID: id, Status: StatusPending, UpdatedAt: now}
	}
	return snap
}

// recover gives every interrupted task a definite, stable destination:
//   - running  -> the interrupted attempt is recorded; if another attempt
//     is allowed the task becomes ready (and will be retried), otherwise
//     it is failed (retries_exhausted).
//   - retrying -> ready (the backoff wait does not survive a restart).
//   - pending tasks whose dependencies died are skipped transitively.
//
// All recovery transitions are validated and persisted before Run resumes.
func (e *Engine) recover() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	changed := false
	for _, id := range e.plan.IDs() {
		st := e.states[id]
		switch st.Status {
		case StatusRunning:
			def, _ := e.plan.Task(id)
			rec := AttemptRecord{
				Attempt:   st.Attempt,
				Reason:    ReasonInterrupted,
				Err:       "process interrupted while attempt was in flight; outcome unknown",
				StartedAt: st.UpdatedAt,
				EndedAt:   e.clock.Now(),
			}
			st.Records = append(st.Records, rec)
			if st.Attempt >= maxAttempts(def.Retry) {
				st.Status = StatusFailed
				st.LastReason = ReasonRetriesExhausted
				st.UpdatedAt = e.clock.Now()
				e.log.Printf("[recovery] task=%s decision=terminal running attempt interrupted and budget exhausted -> failed(records=%d)", id, len(st.Records))
			} else {
				st.Status = StatusReady
				st.LastReason = ReasonInterrupted
				st.UpdatedAt = e.clock.Now()
				e.log.Printf("[recovery] task=%s decision=retry interrupted attempt recorded; next attempt=%d/%d -> ready",
					id, st.Attempt+1, maxAttempts(def.Retry))
			}
			changed = true
		case StatusRetrying:
			st.Status = StatusReady
			st.UpdatedAt = e.clock.Now()
			changed = true
			e.log.Printf("[recovery] task=%s decision=retry backoff does not survive restart -> ready (attempt=%d)", id, st.Attempt)
		}
	}

	// Transitive skip propagation for dependencies that died before the
	// crash was persisted; topological order reaches a fixpoint in passes.
	for {
		progress := false
		for _, id := range e.plan.IDs() {
			st := e.states[id]
			if st.Status != StatusPending && st.Status != StatusReady {
				continue
			}
			def, _ := e.plan.Task(id)
			if !st.Attempted() {
				if deadDep := firstDeadDependency(def, e.states); deadDep != "" {
					st.Status = StatusSkipped
					st.LastReason = ReasonDependencyFailed
					st.UpdatedAt = e.clock.Now()
					progress = true
					changed = true
					e.log.Printf("[recovery] task=%s decision=skip dependency %q is terminal-unsuccessful -> skipped", id, deadDep)
				}
			}
		}
		if !progress {
			break
		}
	}

	if changed {
		if err := e.persistLocked(); err != nil {
			return err
		}
		e.log.Printf("[recovery] run=%q decision=persist-recovered-state recovery transitions durable", e.runID)
	}
	return nil
}

// Run executes the DAG concurrently until every task is terminal, then
// returns nil; otherwise it returns *RunError describing the failures.
func (e *Engine) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		cancel()
		return ErrEngineClosed
	}
	runCancel := cancel
	e.mu.Unlock()
	defer runCancel()

	e.log.Printf("[scheduler] run=%q decision=start plan-hash=%s tasks=%d", e.runID, e.plan.Hash(), len(e.plan.IDs()))

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return ErrEngineClosed
		}
		launch := e.schedulePassLocked()
		allDone := e.allTerminalLocked()
		e.mu.Unlock()

		for _, id := range launch {
			e.startAttempt(ctx, id)
		}
		if allDone {
			e.wg.Wait()
			return e.resultError()
		}

		select {
		case <-e.wakeCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// schedulePassLocked makes all immediately possible state transitions:
// pending tasks become ready when dependencies succeeded, skipped when a
// dependency is terminal-unsuccessful; ready tasks are flipped to running
// (persisted before execution) and returned for launching.
func (e *Engine) schedulePassLocked() []string {
	var launch []string
	e.promotePendingLocked()
	for _, id := range e.plan.IDs() {
		st := e.states[id]
		if st.Status == StatusReady && !e.queued[id] {
			e.queued[id] = true
			launch = append(launch, id)
		}
	}
	if err := e.persistLocked(); err != nil {
		// Persistence failure is fatal: do not advance from an
		// unpersisted boundary.
		e.log.Printf("[scheduler] decision=abort persistence failed before dispatch: %v", err)
	}
	return launch
}

// promotePendingLocked applies every pending->ready / pending->skipped
// transition whose dependency condition now holds.
func (e *Engine) promotePendingLocked() {
	for _, id := range e.plan.IDs() {
		st := e.states[id]
		if st.Status != StatusPending {
			continue
		}
		def, _ := e.plan.Task(id)
		if dead := firstDeadDependency(def, e.states); dead != "" {
			st.Status = StatusSkipped
			st.LastReason = ReasonDependencyFailed
			st.UpdatedAt = e.clock.Now()
			e.log.Printf("[scheduler] task=%s decision=skip because dependency=%s is failed/skipped -> skipped", id, dead)
			continue
		}
		if allDepsSucceeded(def, e.states) {
			st.Status = StatusReady
			st.UpdatedAt = e.clock.Now()
			e.log.Printf("[scheduler] task=%s decision=ready all %d dependencies succeeded -> ready", id, len(def.Deps))
		}
	}
}

// startAttempt flips the task to running durably and supervises one
// attempt. It is safe against duplicate/concurrent triggering: the queued
// guard plus the status check ensure a task's attempt is launched once.
func (e *Engine) startAttempt(ctx context.Context, id string) {
	if e.sem != nil {
		select {
		case e.sem <- struct{}{}:
		case <-ctx.Done():
			e.mu.Lock()
			delete(e.queued, id)
			e.mu.Unlock()
			return
		}
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		if e.sem != nil {
			defer func() { <-e.sem }()
		}

		def, ok := e.plan.Task(id)
		if !ok {
			return
		}

		e.mu.Lock()
		st := e.states[id]
		if st.Status != StatusReady {
			// Lost a race (e.g. duplicate TriggerTask); never run twice.
			delete(e.queued, id)
			e.mu.Unlock()
			e.log.Printf("[scheduler] task=%s decision=skip-launch status is %s not ready", id, st.Status)
			return
		}
		st.Attempt++
		st.Status = StatusRunning
		st.UpdatedAt = e.clock.Now()
		startedAt := e.clock.Now()
		if err := e.persistLocked(); err != nil {
			st.Status = StatusReady
			st.Attempt--
			st.UpdatedAt = e.clock.Now()
			delete(e.queued, id)
			e.mu.Unlock()
			e.log.Printf("[scheduler] task=%s decision=hold persistence of running state failed: %v", id, err)
			e.signal()
			return
		}
		attempt := st.Attempt
		delete(e.queued, id)
		e.mu.Unlock()
		e.log.Printf("[state] task=%s transition ready->running attempt=%d/%d (running state persisted before invocation)",
			id, attempt, maxAttempts(def.Retry))

		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		outcome := e.executeAttempt(runCtx, cancel, def, attempt, startedAt)
		e.applyOutcome(ctx, id, outcome)
	}()
}

type attemptOutcome struct {
	attempt   int
	startedAt time.Time
	err       error
	reason    FailureReason
	timedOut  bool
}

// executeAttempt runs one task function with an optional attempt timeout.
// Time is driven entirely by the injected clock, so tests control timeouts
// deterministically without sleeping.
func (e *Engine) executeAttempt(ctx context.Context, cancel context.CancelFunc, def TaskDef, attempt int, startedAt time.Time) attemptOutcome {
	resCh := make(chan error, 1)
	if def.Run == nil {
		return attemptOutcome{attempt: attempt, startedAt: startedAt,
			err:    fmt.Errorf("task %q has no runnable function", def.ID),
			reason: ReasonNonRetryable}
	}
	go func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("task panicked: %v", r)
			}
			resCh <- err
		}()
		err = def.Run(ctx)
	}()

	var timer Timer
	var timeoutCh <-chan time.Time
	if def.Timeout > 0 {
		timer = e.clock.NewTimer(def.Timeout)
		timeoutCh = timer.C()
	}
	select {
	case err := <-resCh:
		if timer != nil {
			timer.Stop()
		}
		o := attemptOutcome{attempt: attempt, startedAt: startedAt, err: err}
		if err != nil {
			if IsNonRetryable(err) {
				o.reason = ReasonNonRetryable
			}
		}
		return o
	case <-timeoutCh:
		// The attempt exceeded its deadline. Cancel its context so the
		// handler observes the deadline; the outcome is already settled
		// deterministically as a timeout regardless of when (or whether)
		// the handler returns.
		cancel()
		o := attemptOutcome{attempt: attempt, startedAt: startedAt,
			timedOut: true, reason: ReasonTimeout,
			err: fmt.Errorf("attempt %d timed out after %s", attempt, def.Timeout)}
		e.log.Printf("[scheduler] task=%s decision=timeout attempt=%d exceeded %s", def.ID, attempt, def.Timeout)
		// Drain the eventual handler completion so it cannot block.
		go func() { <-resCh }()
		return o
	case <-ctx.Done():
		if timer != nil {
			timer.Stop()
		}
		return attemptOutcome{attempt: attempt, startedAt: startedAt,
			err: ctx.Err(), reason: ReasonNonRetryable}
	}
}

// applyOutcome is the single state-transition point after an attempt.
func (e *Engine) applyOutcome(ctx context.Context, id string, o attemptOutcome) {
	def, _ := e.plan.Task(id)

	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.states[id]

	rec := AttemptRecord{
		Attempt:   o.attempt,
		StartedAt: o.startedAt,
		EndedAt:   e.clock.Now(),
		Timeout:   o.timedOut,
		Reason:    o.reason,
	}
	if o.err != nil {
		rec.Err = o.err.Error()
	}
	st.Records = append(st.Records, rec)

	retryable := false
	switch {
	case o.err == nil:
		st.Status = StatusSucceeded
		st.LastReason = ReasonNone
		e.log.Printf("[state] task=%s transition running->succeeded attempt=%d (exactly one success becomes effective)", id, o.attempt)
	case o.reason == ReasonNonRetryable:
		st.Status = StatusFailed
		st.LastReason = ReasonNonRetryable
		e.log.Printf("[state] task=%s transition running->failed attempt=%d reason=non_retryable_error", id, o.attempt)
	case o.timedOut && !def.Retry.TimeoutIsRetryable:
		st.Status = StatusFailed
		st.LastReason = ReasonTimeout
		e.log.Printf("[state] task=%s transition running->failed attempt=%d reason=timeout terminal", id, o.attempt)
	case o.attempt >= maxAttempts(def.Retry):
		st.Status = StatusFailed
		st.LastReason = ReasonRetriesExhausted
		e.log.Printf("[state] task=%s transition running->failed attempt=%d/%d reason=retries_exhausted",
			id, o.attempt, maxAttempts(def.Retry))
	default:
		retryable = true
		st.Status = StatusRetrying
		st.LastReason = o.reason
		if st.LastReason == ReasonNone {
			st.LastReason = ReasonRetryableError
		}
		e.log.Printf("[state] task=%s transition running->retrying attempt=%d/%d scheduling backoff=%s",
			id, o.attempt, maxAttempts(def.Retry), backoffDelay(def.Retry))
	}
	st.UpdatedAt = e.clock.Now()
	if st.Status == StatusFailed {
		e.skipDependentsLocked(id)
	}
	if err := e.persistLocked(); err != nil {
		e.log.Printf("[scheduler] task=%s decision=alarm outcome transition could not be persisted: %v", id, err)
	}

	if retryable {
		delay := backoffDelay(def.Retry)
		if delay <= 0 {
			// Immediate retry: re-home to ready without a timer so a fake
			// clock that nobody advances still makes progress.
			st.Status = StatusReady
			st.UpdatedAt = e.clock.Now()
			if err := e.persistLocked(); err != nil {
				e.log.Printf("[scheduler] task=%s decision=alarm retry ready-state not persisted: %v", id, err)
			}
			e.log.Printf("[state] task=%s transition retrying->ready immediately attempt=%d", id, st.Attempt)
		} else {
			backoffTimer := e.clock.NewTimer(delay)
			e.wg.Add(1)
			go func() {
				defer e.wg.Done()
				select {
				case <-backoffTimer.C():
					e.mu.Lock()
					cur := e.states[id]
					if cur.Status == StatusRetrying {
						cur.Status = StatusReady
						cur.UpdatedAt = e.clock.Now()
						if err := e.persistLocked(); err != nil {
							e.log.Printf("[scheduler] task=%s decision=alarm retry ready-state not persisted: %v", id, err)
						}
						e.log.Printf("[scheduler] task=%s transition retrying->ready after backoff=%s attempt=%d", id, delay, cur.Attempt)
						e.mu.Unlock()
						e.signal()
					} else {
						e.mu.Unlock()
					}
				case <-ctx.Done():
					backoffTimer.Stop()
				}
			}()
		}
	}
	e.signal()
}

// skipDependentsLocked marks every not-yet-attempted transitive dependent
// as skipped with reason dependency_failed.
func (e *Engine) skipDependentsLocked(failedID string) {
	queue := []string{failedID}
	seen := map[string]bool{failedID: true}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, id := range e.plan.IDs() {
			def, _ := e.plan.Task(id)
			if !containsString(def.Deps, cur) {
				continue
			}
			st := e.states[id]
			if !st.Attempted() && st.Status != StatusSkipped && !seen[id] {
				st.Status = StatusSkipped
				st.LastReason = ReasonDependencyFailed
				st.UpdatedAt = e.clock.Now()
				seen[id] = true
				queue = append(queue, id)
				e.log.Printf("[state] task=%s transition %s->skipped dependency=%s failed transitively",
					id, st.Status, cur)
			}
		}
	}
}

// TriggerTask runs one specific task. The task must currently be ready
// (all dependencies succeeded). Duplicate or concurrent triggering is
// rejected with a distinguishable error instead of executing twice.
func (e *Engine) TriggerTask(ctx context.Context, id string) error {
	def, ok := e.plan.Task(id)
	if !ok {
		return fmt.Errorf("%w: %q", ErrTaskNotFound, id)
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrEngineClosed
	}
	st := e.states[id]
	switch st.Status {
	case StatusSucceeded:
		e.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrAlreadySucceeded, id)
	case StatusRunning:
		e.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrAlreadyRunning, id)
	case StatusFailed, StatusSkipped:
		e.mu.Unlock()
		return fmt.Errorf("%w: %q is %s", ErrTerminal, id, st.Status)
	case StatusRetrying:
		e.mu.Unlock()
		return fmt.Errorf("%w: %q is waiting for retry backoff", ErrNotReady, id)
	case StatusPending:
		if dead := firstDeadDependency(def, e.states); dead != "" {
			e.mu.Unlock()
			return fmt.Errorf("%w: %q dependency %q did not succeed", ErrNotReady, id, dead)
		}
		if !allDepsSucceeded(def, e.states) {
			e.mu.Unlock()
			return fmt.Errorf("%w: %q dependencies are not all succeeded yet", ErrNotReady, id)
		}
		st.Status = StatusReady
		st.UpdatedAt = e.clock.Now()
		if err := e.persistLocked(); err != nil {
			e.mu.Unlock()
			return err
		}
		e.log.Printf("[scheduler] task=%s decision=ready (manual trigger) dependencies satisfied", id)
	case StatusReady:
	}
	if e.queued[id] {
		e.mu.Unlock()
		return fmt.Errorf("%w: %q launch already queued", ErrAlreadyRunning, id)
	}
	e.queued[id] = true
	e.mu.Unlock()

	// Triggered attempts are bound to the engine's own lifecycle context,
	// not to the (short-lived) caller context.
	e.startAttempt(e.bgCtx, id)
	return nil
}

// State returns a copy of a task's current state.
func (e *Engine) State(id string) (TaskState, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.states[id]
	if !ok {
		return TaskState{}, false
	}
	return *cloneTaskState(st), true
}

// Snapshot returns a point-in-time deep copy of the whole run state.
func (e *Engine) Snapshot() Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	snap := Snapshot{
		RunID:     e.runID,
		PlanHash:  e.plan.Hash(),
		CreatedAt: e.created,
		UpdatedAt: e.clock.Now(),
		Tasks:     make(map[string]*TaskState, len(e.states)),
	}
	for id, st := range e.states {
		snap.Tasks[id] = cloneTaskState(st)
	}
	return snap
}

// Wait blocks until all tasks reach a terminal state or ctx is cancelled.
func (e *Engine) Wait(ctx context.Context) error {
	for {
		e.mu.Lock()
		done := e.allTerminalLocked()
		e.mu.Unlock()
		if done {
			e.wg.Wait()
			return nil
		}
		select {
		case <-e.wakeCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close cancels in-flight attempts and marks the engine closed. Persisted
// "running" tasks are recovered deterministically on the next NewEngine.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	cancel := e.cancelFn
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// --------------------------------------------------------------- internals

func (e *Engine) signal() {
	select {
	case e.wakeCh <- struct{}{}:
	default:
	}
}

func (e *Engine) allTerminalLocked() bool {
	for _, st := range e.states {
		switch st.Status {
		case StatusSucceeded, StatusFailed, StatusSkipped:
		default:
			return false
		}
	}
	return true
}

func (e *Engine) resultError() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var runErr RunError
	for _, id := range e.plan.IDs() {
		st := e.states[id]
		if st.Status == StatusSucceeded {
			continue
		}
		te := TaskError{ID: id, Status: st.Status, Reason: st.LastReason, Attempts: st.Attempt}
		if len(st.Records) > 0 {
			te.LastError = st.Records[len(st.Records)-1].Err
		}
		runErr.Tasks = append(runErr.Tasks, te)
	}
	if len(runErr.Tasks) == 0 {
		return nil
	}
	return &runErr
}

// persistLocked serializes and durably saves the current state. Caller
// holds e.mu; the persistence boundary is the only thing a restart can see.
func (e *Engine) persistLocked() error {
	now := e.clock.Now()
	snap := &Snapshot{
		RunID:     e.runID,
		PlanHash:  e.plan.Hash(),
		CreatedAt: e.created,
		UpdatedAt: now,
		Tasks:     make(map[string]*TaskState, len(e.states)),
	}
	for id, st := range e.states {
		snap.Tasks[id] = cloneTaskState(st)
	}
	if err := validateSnapshot(snap, e.plan); err != nil {
		return err
	}
	if err := e.store.Save(context.Background(), snap); err != nil {
		return err
	}
	return nil
}

func allDepsSucceeded(def TaskDef, states map[string]*TaskState) bool {
	for _, dep := range def.Deps {
		if states[dep].Status != StatusSucceeded {
			return false
		}
	}
	return true
}

func firstDeadDependency(def TaskDef, states map[string]*TaskState) string {
	for _, dep := range def.Deps {
		switch states[dep].Status {
		case StatusFailed, StatusSkipped:
			return dep
		}
	}
	return ""
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func cloneTaskState(st *TaskState) *TaskState {
	cp := *st
	if st.Records != nil {
		cp.Records = append([]AttemptRecord(nil), st.Records...)
	}
	return &cp
}

// Attempted reports whether the task ever started an attempt.
func (st *TaskState) Attempted() bool { return st.Attempt > 0 }

type discardLogger struct{}

func (discardLogger) Printf(format string, args ...any) {}

// testLogLogger adapts *testing.T to Logger.
type testLogLogger struct{ l testLogger }

type testLogger interface {
	Logf(format string, args ...any)
}

func newTestLogger(l testLogger) Logger { return &testLogLogger{l: l} }

func (t *testLogLogger) Printf(format string, args ...any) {
	t.l.Logf(format, args...)
}
