package orchestrator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Logger receives human-readable scheduling-decision and state-transition
// evidence. Tests plug in a verbose logger so unit-test logs explain every
// decision.
type Logger interface {
	Printf(format string, args ...any)
}

// Options configures an Engine.
type Options struct {
	Store         Store
	Executor      Executor
	Clock         Clock // defaults to SystemClock
	MaxConcurrent int   // <= 0 => len(defs)
	Logger        Logger
}

// TerminalError is returned by Trigger when the task can never run.
var (
	ErrDepsNotSatisfied = errors.New("orchestrator: dependencies are not all satisfied yet")
	ErrDependencyFailed = errors.New("orchestrator: a dependency has failed terminally")
	ErrWaitTimeout      = errors.New("orchestrator: wait timed out before all tasks settled")
)

type eventKind int

const (
	evAttemptDone eventKind = iota
	evRetryDue
	evTrigger
)

type event struct {
	kind   eventKind
	taskID string
	result *Result // evAttemptDone: nil means timeout
	seq    int
	at     time.Time // evRetryDue
	reply  chan error
}

// Engine drives scheduling, retries, timeouts and restart recovery.
//
// All state mutation happens on a single scheduler goroutine (actor model),
// started by Run. This is what makes duplicate triggers / concurrent worker
// replies unable to produce a double-committed effect: there is exactly one
// place a task can leave the "running" state.
type Engine struct {
	order []string
	defs  map[string]TaskDef

	store    Store
	executor Executor
	clock    Clock
	log      Logger
	maxConc  int

	// mu guards snap for concurrent readers (State/Snapshot). The scheduler
	// goroutine is the only writer; it takes the write lock per decision.
	mu   sync.RWMutex
	snap *Snapshot

	events  chan event
	settled chan struct{}
	stop    chan struct{}
	sem     chan struct{}

	runStarted atomic.Bool

	// execCtx / execCancel control all in-flight executor attempts and are
	// wired to the context passed to Run.
	execCtx    context.Context
	execCancel context.CancelFunc

	// triggerWaiters are explicit Trigger() calls parked on a pending task
	// whose dependencies are satisfied; they are resolved when the task is
	// dispatched or reaches a terminal state.
	waiters map[string][]chan error
}

// New validates the DAG, loads persisted state (or initializes and commits
// it), applies crash recovery, and prepares an engine. No task runs until
// Run is called.
func New(defs []TaskDef, opt Options) (*Engine, error) {
	if opt.Store == nil {
		return nil, errors.New("orchestrator: Options.Store is required")
	}
	if opt.Executor == nil {
		return nil, errors.New("orchestrator: Options.Executor is required")
	}
	if err := validateDAG(defs); err != nil {
		return nil, err
	}
	clock := opt.Clock
	if clock == nil {
		clock = NewSystemClock()
	}
	maxConc := opt.MaxConcurrent
	if maxConc <= 0 {
		maxConc = len(defs)
	}
	if maxConc > len(defs) {
		maxConc = len(defs)
	}

	index := make(map[string]TaskDef, len(defs))
	order := make([]string, 0, len(defs))
	for _, d := range defs {
		n := d
		n.Retry = d.Retry.normalized()
		index[d.ID] = n
		order = append(order, d.ID)
	}

	e := &Engine{
		order:    order,
		defs:     index,
		store:    opt.Store,
		executor: opt.Executor,
		clock:    clock,
		log:      opt.Logger,
		maxConc:  maxConc,
		events:   make(chan event, 256),
		settled:  make(chan struct{}),
		stop:     make(chan struct{}),
		sem:      make(chan struct{}, maxConc),
		waiters:  make(map[string][]chan error),
	}

	loaded, err := opt.Store.Load()
	if err != nil {
		if !errors.Is(err, ErrNoSnapshot) {
			return nil, err
		}
		// First ever start: every task is pending. Committing version 0 makes
		// the initial state itself durable and auditable.
		now := clock.Now()
		snap := &Snapshot{Version: 0, Tasks: make(map[string]*TaskState, len(order))}
		for _, id := range order {
			snap.Tasks[id] = newInitialState(now)
		}
		if err := validateSnapshot(snap, index); err != nil {
			return nil, err
		}
		if err := opt.Store.Commit(snap); err != nil {
			return nil, err
		}
		loaded = snap
		e.decide("initialized fresh state with %d pending tasks at version %d", len(order), loaded.Version)
	} else {
		if err := validateSnapshot(loaded, index); err != nil {
			return nil, err
		}
		e.decide("loaded persisted state at version %d with %d tasks", loaded.Version, len(loaded.Tasks))
	}
	e.snap = loaded

	if err := e.recover(); err != nil {
		return nil, err
	}
	return e, nil
}

// decidef is the single logging chokepoint: every scheduling decision and
// state transition is explained through it.
func (e *Engine) decide(format string, args ...any) {
	if e.log != nil {
		e.log.Printf("[orchestrator] "+format, args...)
	}
}

// Run starts the scheduler loop and blocks until all tasks reach a terminal
// state or ctx is cancelled. It must be called at most once per Engine.
func (e *Engine) Run(ctx context.Context) error {
	return e.runLoop(ctx)
}

// Trigger explicitly requests scheduling of a task whose dependencies have
// all succeeded. It is idempotent: re-triggering a task that is already
// running, waiting for retry, or terminal returns *ErrTriggerIgnored rather
// than starting a duplicate execution.
func (e *Engine) Trigger(ctx context.Context, taskID string) error {
	return e.trigger(ctx, taskID)
}

// State returns the current state of a task (safe to call concurrently).
func (e *Engine) State(taskID string) (TaskState, error) {
	if _, ok := e.defs[taskID]; !ok {
		return TaskState{}, &ValidationError{Reason: ReasonInvalidTask, TaskID: taskID, Detail: "unknown task"}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	st := e.snap.Tasks[taskID]
	cp := *st
	if st.Attempts != nil {
		cp.Attempts = append([]Attempt(nil), st.Attempts...)
	}
	return cp, nil
}

// Snapshot returns a copy of the full current state.
func (e *Engine) Snapshot() Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := Snapshot{Version: e.snap.Version, Tasks: make(map[string]*TaskState, len(e.snap.Tasks))}
	for id, st := range e.snap.Tasks {
		c := *st
		if st.Attempts != nil {
			c.Attempts = append([]Attempt(nil), st.Attempts...)
		}
		cp.Tasks[id] = &c
	}
	return cp
}

// Wait blocks until the engine settles (all tasks terminal) or timeout
// elapses. A non-positive timeout waits forever.
func (e *Engine) Wait(timeout time.Duration) error {
	if timeout <= 0 {
		<-e.settled
		return nil
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-e.settled:
		return nil
	case <-t.C:
		return ErrWaitTimeout
	}
}
