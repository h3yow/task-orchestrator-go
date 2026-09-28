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
	evShutdown
	evWorkerExit
)

type event struct {
	kind   eventKind
	taskID string
	result *Result // evAttemptDone: nil means timeout
	seq    int
	at     time.Time // evRetryDue
	reply  chan error
	// epoch tags timer-born events (retry due, attempt timeout delivery) with
	// the Run generation that armed the timer. A timer goroutine that outlives
	// its Run (e.g. an engine abandoned to simulate a hard crash) must not
	// deliver into a later engine/generation: such stale events are dropped.
	// retryGen is the per-task backoff generation carried by evRetryDue, so a
	// timer event that arrives late (after the task already entered a newer
	// wait cycle or got dispatched) cannot spuriously dispatch it.
	epoch    int
	retryGen int
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

	// shutdownCh is closed by the scheduler once a graceful shutdown has fully
	// completed (Run is about to return). Shutdown callers block on it.
	shutdownCh chan struct{}

	runStarted atomic.Bool

	// execCtx / execCancel control all in-flight executor attempts and are
	// wired to the context passed to Run.
	execCtx    context.Context
	execCancel context.CancelFunc

	// triggerWaiters are explicit Trigger() calls parked on a pending task
	// whose dependencies are satisfied; they are resolved when the task is
	// dispatched or reaches a terminal state.
	waiters map[string][]chan error

	// retryArmed tracks the tasks whose retry timer is currently registered,
	// so repeated scheduling passes never stack duplicate timers. It is only
	// touched by the scheduler goroutine. Timers must be re-registered after a
	// restart because the old ones died with the previous process; the awaited
	// time itself comes from the persisted absolute NextRunAt, so a restart
	// never restarts the wait clock.
	retryArmed map[string]bool
	// retryGens is the current backoff generation of each task's retry timer.
	// It is bumped every time a fresh backoff timer is armed, so an evRetryDue
	// from an older wait cycle is recognizably stale even within one Run.
	retryGens map[string]int

	// Shutdown bookkeeping; touched only by the scheduler goroutine except for
	// shutdownRequested (set once via Shutdown before the event is enqueued).
	shutdownRequested atomic.Bool
	draining          bool
	inFlight          int
	shutdownReply     chan error

	// epoch identifies the current Run generation; touched only by the
	// scheduler goroutine. Timer goroutines armed during a Run carry its epoch.
	epoch int
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

	digest, err := planDigest(index, order)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		order:      order,
		defs:       index,
		store:      opt.Store,
		executor:   opt.Executor,
		clock:      clock,
		log:        opt.Logger,
		maxConc:    maxConc,
		events:     make(chan event, 256),
		settled:    make(chan struct{}),
		stop:       make(chan struct{}),
		sem:        make(chan struct{}, maxConc),
		shutdownCh: make(chan struct{}),
		waiters:    make(map[string][]chan error),
		retryArmed: make(map[string]bool),
		retryGens:  make(map[string]int),
	}

	loaded, err := opt.Store.Load()
	if err != nil {
		if !errors.Is(err, ErrNoSnapshot) {
			return nil, err
		}
		// First ever start: every task is pending. Committing version 0 makes
		// the initial state itself durable and auditable.
		now := clock.Now()
		snap := &Snapshot{Version: 0, PlanDigest: digest, Tasks: make(map[string]*TaskState, len(order))}
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
		e.decide("initialized fresh state with %d pending tasks at version %d (plan digest %s)",
			len(order), loaded.Version, shortDigest(digest))
	} else {
		// Plan identity is checked before structural validation: a plan change
		// and a corrupted/rolled-back snapshot must report different reasons.
		if err := checkPlan(loaded, index, digest); err != nil {
			return nil, err
		}
		if err := validateSnapshot(loaded, index); err != nil {
			return nil, err
		}
		e.decide("loaded persisted state at version %d with %d tasks (plan digest %s matches)",
			loaded.Version, len(loaded.Tasks), shortDigest(digest))
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
