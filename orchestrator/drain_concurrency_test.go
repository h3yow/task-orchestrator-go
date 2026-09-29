package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"
)

// gateBatchExecutor runs every task behind a per-task release gate while still
// honoring context cancellation. startBarrier (when non-nil) is signalled once
// per Execute call right before the gate wait, so a test can deterministically
// know an attempt is in flight without polling status.
type gateBatchExecutor struct {
	mu           sync.Mutex
	gates        map[string]chan struct{}
	results      map[string]Result
	startBarrier chan string
}

func newGateBatchExecutor(results map[string]Result) *gateBatchExecutor {
	return &gateBatchExecutor{
		gates:   map[string]chan struct{}{},
		results: results,
	}
}

func (g *gateBatchExecutor) gate(id string) chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	ch, ok := g.gates[id]
	if !ok {
		ch = make(chan struct{})
		g.gates[id] = ch
	}
	return ch
}

func (g *gateBatchExecutor) Execute(ctx context.Context, id string) Result {
	g.mu.Lock()
	ch := g.gates[id]
	r := g.results[id]
	barrier := g.startBarrier
	g.mu.Unlock()
	if barrier != nil {
		select {
		case barrier <- id:
		case <-ctx.Done():
			return Result{OK: false, Retryable: true, Reason: "cancelled before gate"}
		}
	}
	if ch != nil {
		// A closed release gate wins even if ctx was cancelled concurrently
		// (non-blocking check first), so "let it finish naturally within the
		// grace period" stays distinguishable from "force-cancelled at
		// expiry" in the mixed-ending test.
		select {
		case <-ch:
		default:
			select {
			case <-ch:
			case <-ctx.Done():
				// Force-cancelled; the scheduler classifies the attempt from
				// the force signal itself and discards this verdict.
				return Result{OK: false, Retryable: true, Reason: "cancelled at gate"}
			}
		}
	}
	return r
}

// stallingStore wraps a Store and can block the scheduler inside Commit at a
// chosen moment. It is the deterministic timing tool behind the concurrent
// drain tests: with the stall armed, the first attempt-done event processed
// after the drain starts blocks the scheduler mid-reconciliation, while every
// other finishing worker releases its slot and queues its result event in the
// engine's buffered events channel. Releasing the stall then presents the old
// "len(sem)==0" drain check with queued, unreconciled events.
type stallingStore struct {
	Store
	mu      sync.Mutex
	stallCh chan struct{} // non-nil while armed: first Commit blocks here
}

func newStallingStore(inner Store) *stallingStore { return &stallingStore{Store: inner} }

func (s *stallingStore) arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stallCh = make(chan struct{})
}

func (s *stallingStore) release() {
	s.mu.Lock()
	ch := s.stallCh
	s.stallCh = nil
	s.mu.Unlock()
	if ch != nil {
		// Unblocks the waiting Commit; any Commit that races in after this
		// point observes stallCh == nil and passes straight through.
		close(ch)
	}
}

func (s *stallingStore) Commit(snap *Snapshot) error {
	s.mu.Lock()
	ch := s.stallCh
	s.mu.Unlock()
	if ch != nil {
		// Blocks until release() closes the channel. After that, a closed
		// channel never blocks again, and arm()/release() set it back to nil,
		// so subsequent commits run normally.
		<-ch
	}
	return s.Store.Commit(snap)
}

// waitForQueuedAttemptEvents waits until at least n events are pending in the
// scheduler's buffered events channel. All such events are attempt-done
// replies in these tests (retry timers are never armed for one-shot tasks).
func waitForQueuedAttemptEvents(t *testing.T, e *Engine, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(e.events) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d events queued, want >= %d", len(e.events), n)
}

// waitForLog polls the captured decision log for sub (used instead of real
// sleeps to synchronize on scheduler decisions).
func waitForLog(t *testing.T, log *captureLogger, sub string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if log.contains(sub) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("log never contained %q", sub)
}

// assertNoRunningAndConsistent is the core post-drain invariant demanded by
// the concurrent-shutdown fix: no task is left "running", and the drain
// summary's "running=0" claim matches the actual snapshot.
func assertNoRunningAndConsistent(t *testing.T, e *Engine) {
	t.Helper()
	snap := e.Snapshot()
	running := 0
	for id, st := range snap.Tasks {
		switch st.Status {
		case StatusSucceeded, StatusFailed, StatusPending, StatusRetryWait:
		default:
			t.Errorf("task %q has unknown status %q", id, st.Status)
		}
		if st.Status == StatusRunning {
			running++
			t.Errorf("task %q still running after shutdown returned", id)
		}
	}
	// Scheduler-side counter must have converged to zero as well.
	if e.inFlight != 0 {
		t.Errorf("engine inFlight=%d after shutdown, want 0", e.inFlight)
	}
	if running != 0 {
		t.Errorf("drain summary claimed running=0 but snapshot has %d running tasks", running)
	}
}

// independentTaskDefs returns n tasks with no dependencies and one-attempt
// success semantics, each one dispatched automatically at start.
func independentTaskDefs(n int) []TaskDef {
	defs := make([]TaskDef, n)
	for i := range defs {
		defs[i] = TaskDef{ID: taskName(i), Retry: RetryPolicy{MaxAttempts: 1}}
	}
	return defs
}

func taskName(i int) string {
	return "t" + string(rune('a'+i))
}

// TestConcurrentDrainNaturalFinishesNoRunningLeft: many tasks finish at almost
// exactly the moment Shutdown is requested and their result events sit queued
// behind the drain decision. The drain must not complete until every result is
// committed; this is the deterministic reproduction of the early-finish bug
// (the old len(sem)==0 check finished the drain with tasks still "running").
func TestConcurrentDrainNaturalFinishesNoRunningLeft(t *testing.T) {
	const n = 8
	store := newStallingStore(NewMemoryStore())
	defs := independentTaskDefs(n)
	results := make(map[string]Result, n)
	for i := 0; i < n; i++ {
		results[taskName(i)] = Result{OK: true, Reason: "natural finish at shutdown"}
	}
	exec := newGateBatchExecutor(results)
	for i := 0; i < n; i++ {
		exec.gate(taskName(i))
	}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()

	// Wait until every task is in flight.
	for i := 0; i < n; i++ {
		pollStatus(t, e, taskName(i), StatusRunning, 2*time.Second)
	}

	// Arm the commit stall, then request shutdown. The scheduler enters drain
	// mode and runs scheduleAll (which dispatches nothing); no attempt result
	// has been processed yet.
	store.arm()
	shutdownDone := make(chan struct{})
	go func() { _ = e.Shutdown(context.Background()); close(shutdownDone) }()
	waitForLog(t, log, "entering drain mode")

	// Release all attempts at once. Workers deliver concurrently. The first
	// processed result blocks inside Commit; the other n-1 result events queue
	// in the buffered channel with their semaphore slots already released.
	for i := 0; i < n; i++ {
		close(exec.gate(taskName(i)))
	}
	waitForQueuedAttemptEvents(t, e, n-1)

	// Shutdown must still be waiting: queued results are not reconciled yet.
	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned while result events were still queued/uncommitted")
	case <-time.After(50 * time.Millisecond):
	}

	// Let the scheduler drain the queue and commit every result.
	store.release()
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown never returned after results were reconciled")
	}

	assertNoRunningAndConsistent(t, e)
	for i := 0; i < n; i++ {
		st, _ := e.State(taskName(i))
		if st.Status != StatusSucceeded {
			t.Fatalf("task %s status=%s, want succeeded (natural result committed)", taskName(i), st.Status)
		}
	}

	// Restart: every task already has a durable success; none may re-execute.
	exec2 := newScriptedExecutor()
	for i := 0; i < n; i++ {
		exec2.set(taskName(i), Result{OK: true, Reason: "must never run"})
	}
	e2, err := New(defs, Options{Store: store.Store, Executor: exec2, Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if c := exec2.callCount(taskName(i)); c != 0 {
			t.Fatalf("succeeded task %s re-executed %d times after restart", taskName(i), c)
		}
	}
}

// TestConcurrentDrainForcedCancelNoRunningLeft: the grace period expires while
// several attempts are in flight; all are force-cancelled. Their interrupted
// reconciliations land deterministically (retry_wait here, since budget
// remains), and no result is lost or misclassified even when the forced
// attempt-done events all arrive together.
func TestConcurrentDrainForcedCancelNoRunningLeft(t *testing.T) {
	const n = 6
	store := newStallingStore(NewMemoryStore())
	defs := make([]TaskDef, n)
	for i := range defs {
		defs[i] = TaskDef{ID: taskName(i), Retry: RetryPolicy{MaxAttempts: 3}}
	}
	results := make(map[string]Result, n)
	for i := 0; i < n; i++ {
		// The executor would report success if allowed to finish naturally;
		// forced drain must not trust that verdict.
		results[taskName(i)] = Result{OK: true, Reason: "must be ignored after force cancel"}
	}
	exec := newGateBatchExecutor(results)
	for i := 0; i < n; i++ {
		exec.gate(taskName(i))
	}
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	for i := 0; i < n; i++ {
		pollStatus(t, e, taskName(i), StatusRunning, 2*time.Second)
	}

	// Arm the commit stall, then force the drain immediately via an already
	// cancelled context (no real waiting: the grace period is over at once).
	store.arm()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownDone := make(chan struct{})
	go func() { _ = e.Shutdown(ctx); close(shutdownDone) }()

	// All worker goroutines are unblocked by the forceDrain broadcast; the
	// first forced reconciliation stalls in Commit, the rest queue behind it.
	waitForQueuedAttemptEvents(t, e, n-1)
	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned while forced reconciliations were uncommitted")
	case <-time.After(50 * time.Millisecond):
	}
	store.release()
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown never returned after forced reconciliations")
	}

	assertNoRunningAndConsistent(t, e)
	for i := 0; i < n; i++ {
		st, _ := e.State(taskName(i))
		if st.Status != StatusRetryWait {
			t.Fatalf("task %s status=%s, want retry_wait after forced cancel with budget left", taskName(i), st.Status)
		}
		a := st.Attempts[len(st.Attempts)-1]
		if a.Class != AttemptInterrupted {
			t.Fatalf("task %s last attempt class=%s, want interrupted", taskName(i), a.Class)
		}
	}

	// Restart: interrupted tasks are eligible immediately and each runs once;
	// the natural "success" verdict observed during force-cancel must NOT have
	// been committed as a success.
	exec2 := newScriptedExecutor()
	for i := 0; i < n; i++ {
		exec2.set(taskName(i), Result{OK: true, Reason: "after restart"})
	}
	e2, err := New(defs, Options{Store: store.Store, Executor: exec2, Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		st, _ := e2.State(taskName(i))
		if st.Status != StatusSucceeded {
			t.Fatalf("task %s status=%s after restart, want succeeded", taskName(i), st.Status)
		}
		if c := exec2.callCount(taskName(i)); c != 1 {
			t.Fatalf("task %s executed %d times after restart, want exactly 1", taskName(i), c)
		}
	}
}

// TestConcurrentDrainMixedEndings: one shutdown concurrently reconciles tasks
// that finish naturally within the grace period, tasks force-cancelled when it
// expires (one with budget left -> retry_wait, one on its last attempt ->
// failed), and a task whose dependency only succeeded during the drain, so it
// must stay pending. Every result is accounted for exactly once.
func TestConcurrentDrainMixedEndings(t *testing.T) {
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "nat1", Retry: RetryPolicy{MaxAttempts: 1}},
		{ID: "nat2", Retry: RetryPolicy{MaxAttempts: 1}},
		{ID: "wait1", Retry: RetryPolicy{MaxAttempts: 3}},
		{ID: "last1", Retry: RetryPolicy{MaxAttempts: 1}},
		{ID: "dep", Deps: []string{"nat1"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}
	ids := []string{"nat1", "nat2", "wait1", "last1"}
	results := map[string]Result{
		"nat1":  {OK: true, Reason: "natural 1"},
		"nat2":  {OK: true, Reason: "natural 2"},
		"wait1": {OK: true, Reason: "ignored: forced"},
		"last1": {OK: true, Reason: "ignored: forced"},
	}
	exec := newGateBatchExecutor(results)
	for _, id := range ids {
		exec.gate(id)
	}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	for _, id := range ids {
		pollStatus(t, e, id, StatusRunning, 2*time.Second)
	}

	// Request shutdown with a short grace period. Before the period expires,
	// release nat1/nat2 so they finish naturally (their result events race the
	// grace timer together); wait1/last1 stay gated and get force-cancelled
	// when the period ends. (This mixed path necessarily observes a tiny real
	// grace period; the pure natural/forced tests above use zero real waits.)
	sctx, scancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer scancel()
	shutdownDone := make(chan struct{})
	go func() { _ = e.Shutdown(sctx); close(shutdownDone) }()
	waitForLog(t, log, "entering drain mode")
	close(exec.gate("nat1"))
	close(exec.gate("nat2"))
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown never returned after mixed reconciliations")
	}

	assertNoRunningAndConsistent(t, e)

	want := map[string]Status{
		"nat1":  StatusSucceeded,
		"nat2":  StatusSucceeded,
		"wait1": StatusRetryWait,
		"last1": StatusFailed,
		"dep":   StatusPending,
	}
	for id, s := range want {
		st, _ := e.State(id)
		if st.Status != s {
			t.Fatalf("task %s status=%s, want %s", id, st.Status, s)
		}
	}
	if st, _ := e.State("last1"); st.Attempts[len(st.Attempts)-1].Class != AttemptInterrupted {
		t.Fatalf("last1 terminal attempt class=%s, want interrupted", st.Attempts[len(st.Attempts)-1].Class)
	}
	if st, _ := e.State("dep"); len(st.Attempts) != 0 {
		t.Fatalf("dep must never be dispatched during drain, attempts=%d", len(st.Attempts))
	}
	if !log.contains("succeeded=2 failed=1 pending=1 retry_wait=1 running=0") {
		t.Fatalf("drain summary stats do not match state. log tail:\n%s", log.String())
	}

	// Wait for the old scheduler goroutine to have fully stopped before a new
	// engine opens the same store.
	select {
	case <-e.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("old scheduler did not stop after drain")
	}

	// Restart: succeeded tasks do not rerun; dep runs once; wait1 replays once.
	exec2 := newScriptedExecutor()
	exec2.set("nat1", Result{OK: true}).set("nat2", Result{OK: true})
	exec2.set("wait1", Result{OK: true, Reason: "replayed after restart"})
	exec2.set("dep", Result{OK: true, Reason: "dep ran once"})
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		id  string
		ran int
	}{
		{"nat1", 0}, {"nat2", 0}, {"last1", 0}, {"wait1", 1}, {"dep", 1},
	} {
		if got := exec2.callCount(c.id); got != c.ran {
			t.Fatalf("task %s ran %d times after restart, want %d", c.id, got, c.ran)
		}
	}
}

// TestConcurrentDrainSingleInFlightNaturalFinishes: the single-task version of
// the near-simultaneous window — the attempt's slot is released and its result
// delivered right when the drain decision happens. The drain must still wait
// for the result to be committed. Uses the same deterministic commit stall.
func TestConcurrentDrainSingleInFlightNaturalFinishes(t *testing.T) {
	store := newStallingStore(NewMemoryStore())
	defs := []TaskDef{{ID: "solo", Retry: RetryPolicy{MaxAttempts: 2}}}
	results := map[string]Result{"solo": {OK: true, Reason: "finished alone in the window"}}
	exec := newGateBatchExecutor(results)
	exec.gate("solo")
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "solo", StatusRunning, 2*time.Second)

	// Shutdown with an unbounded context enters drain; the attempt is still
	// gated. Then release it while the single reconciliation is held in
	// Commit — the result event can only complete once the commit runs.
	shutdownDone := make(chan struct{})
	go func() { _ = e.Shutdown(context.Background()); close(shutdownDone) }()
	waitForLog(t, log, "entering drain mode")
	store.arm()
	close(exec.gate("solo"))

	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned while the single in-flight result was uncommitted")
	case <-time.After(50 * time.Millisecond):
	}
	store.release()
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown never returned")
	}

	assertNoRunningAndConsistent(t, e)
	st, _ := e.State("solo")
	if st.Status != StatusSucceeded || st.Attempts[0].Class != AttemptSucceeded {
		t.Fatalf("solo=%+v, want one committed natural success", st)
	}

	// Restart: the durable success must not be replayed.
	exec2 := newScriptedExecutor().set("solo", Result{OK: true})
	e2, err := New(defs, Options{Store: store.Store, Executor: exec2, Clock: NewFakeClock(time.Unix(5, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c := exec2.callCount("solo"); c != 0 {
		t.Fatalf("solo re-executed %d times after restart", c)
	}
}

// TestConcurrentDrainSingleInFlightForced: a single in-flight attempt that is
// force-cancelled at grace expiry is reconciled as interrupted (retry_wait
// here), and the restart replays it exactly once.
func TestConcurrentDrainSingleInFlightForced(t *testing.T) {
	store := newStallingStore(NewMemoryStore())
	defs := []TaskDef{{ID: "solo", Retry: RetryPolicy{MaxAttempts: 2}}}
	results := map[string]Result{"solo": {OK: true, Reason: "ignored after force"}}
	exec := newGateBatchExecutor(results)
	exec.gate("solo")
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "solo", StatusRunning, 2*time.Second)

	store.arm()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownDone := make(chan struct{})
	go func() { _ = e.Shutdown(ctx); close(shutdownDone) }()

	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned while the forced reconciliation was uncommitted")
	case <-time.After(50 * time.Millisecond):
	}
	store.release()
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown never returned")
	}

	assertNoRunningAndConsistent(t, e)
	st, _ := e.State("solo")
	if st.Status != StatusRetryWait || st.Attempts[0].Class != AttemptInterrupted {
		t.Fatalf("solo=%+v, want retry_wait with interrupted attempt", st)
	}

	select {
	case <-e.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("old scheduler did not stop")
	}
	exec2 := newScriptedExecutor().set("solo", Result{OK: true, Reason: "replay"})
	e2, err := New(defs, Options{Store: store.Store, Executor: exec2, Clock: NewFakeClock(time.Unix(5, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c := exec2.callCount("solo"); c != 1 {
		t.Fatalf("solo replayed %d times, want exactly 1", c)
	}
}

// TestDrainCompletesImmediatelyWithNoInFlight: when Shutdown is requested with
// only a retry_wait task (deadline in the future) and a blocked pending task,
// the drain completes immediately — no attempt is in flight, and nothing is
// incorrectly classified or re-run.
func TestDrainCompletesImmediatelyWithNoInFlight(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir + "/state.json")
	defs := []TaskDef{
		{ID: "root", Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Minute}},
		{ID: "down", Deps: []string{"root"}},
	}

	// First run: root fails once (retryable) and parks in retry_wait; then
	// shut down before the backoff elapses.
	exec1 := newScriptedExecutor().
		set("root", Result{OK: false, Retryable: true, Reason: "transient"}).
		set("down", Result{OK: true})
	e1, err := New(defs, Options{Store: store, Executor: exec1, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e1.Run(context.Background()) }()
	st := pollStatus(t, e1, "root", StatusRetryWait, 2*time.Second)
	wantNext := time.Unix(60, 0)
	if !st.NextRunAt.Equal(wantNext) {
		t.Fatalf("NextRunAt=%s want %s", st.NextRunAt, wantNext)
	}

	done := make(chan struct{})
	go func() { _ = e1.Shutdown(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drain with zero in-flight attempts must complete immediately")
	}
	assertNoRunningAndConsistent(t, e1)
	if c := exec1.callCount("down"); c != 0 {
		t.Fatalf("down dispatched %d times during drain", c)
	}

	// Restart before the original deadline: the wait is NOT reset, root does
	// not run early; advance the fake clock past the deadline and it retries.
	clock2 := NewFakeClock(time.Unix(30, 0)) // still 30s before NextRunAt
	exec2 := newScriptedExecutor().set("root", Result{OK: true, Reason: "after backoff"}).set("down", Result{OK: true})
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: clock2, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e2.Run(context.Background()) }()
	time.Sleep(100 * time.Millisecond)
	if c := exec2.callCount("root"); c != 0 {
		t.Fatalf("root retried before original deadline: %d calls", c)
	}
	clock2.Advance(time.Minute)
	pollStatus(t, e2, "down", StatusSucceeded, 2*time.Second)
	if c := exec2.callCount("root"); c != 1 {
		t.Fatalf("root ran %d times after restart, want 1", c)
	}
}
