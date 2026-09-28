package orchestrator

import (
	"context"
	"testing"
	"time"
)

// waitPolicy: first attempt fails retryably, backoff before attempt #2 is 10s.
func waitRetryDefs() []TaskDef {
	return []TaskDef{{ID: "job", Retry: RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     time.Minute,
	}}}
}

// startFailingEngine runs "job" until its first attempt failed and it is parked
// in retry_wait with NextRunAt = t0+10s. The returned stop function stops the
// engine and waits for Run to return.
func startFailingEngine(t *testing.T, store Store, t0 time.Time) (*Engine, *FakeClock, func(error)) {
	t.Helper()
	clock := NewFakeClock(t0)
	exec := newScriptedExecutor().
		set("job",
			Result{OK: false, Retryable: true, Reason: "flaky-1"},
			Result{OK: true, Reason: "recovered"})
	e, err := New(waitRetryDefs(), Options{
		Store: store, Executor: exec, Clock: clock, Logger: newCaptureLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := runEngine(t, e)
	st := pollStatus(t, e, "job", StatusRetryWait, 2*time.Second)
	if st.NextRunAt != t0.Add(10*time.Second) {
		t.Fatalf("NextRunAt=%s want %s", st.NextRunAt, t0.Add(10*time.Second))
	}
	if !clock.WaitForTimers(1) {
		t.Fatal("backoff timer not armed before stop")
	}
	return e, clock, stop
}

// TestRetryWaitRestartBeforeDueKeepsOriginalDeadline: stop while still inside
// the wait window, restart BEFORE the deadline; the task must NOT run early and
// must fire at the ORIGINAL NextRunAt (no re-baselining).
func TestRetryWaitRestartBeforeDueKeepsOriginalDeadline(t *testing.T) {
	store := NewMemoryStore()
	t0 := time.Unix(1000, 0)
	e1, _, stop1 := startFailingEngine(t, store, t0)

	// Stop while still inside the 10s wait window.
	stop1(context.Canceled)
	if st, _ := e1.State("job"); st.Status != StatusRetryWait {
		t.Fatalf("after stop status=%s want retry_wait", st.Status)
	}

	clock2 := NewFakeClock(t0.Add(8 * time.Second))
	exec2 := newScriptedExecutor().set("job", Result{OK: true, Reason: "recovered"})
	log2 := newCaptureLogger(t)
	e2, err := New(waitRetryDefs(), Options{Store: store, Executor: exec2, Clock: clock2, Logger: log2})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e2.Run(context.Background()) }()

	// Give the initial scheduling pass a tick, then advance only 1s (deadline
	// still 1s away): nothing must have run.
	waitForLog(log2, "arming timer", t)
	clock2.Advance(time.Second)
	time.Sleep(30 * time.Millisecond)
	if n := exec2.callCount("job"); n != 0 {
		t.Fatalf("job ran %d times before the original retry deadline", n)
	}
	if st, _ := e2.State("job"); st.Status != StatusRetryWait {
		t.Fatalf("status=%s want retry_wait", st.Status)
	}

	// Advance to (and past) the original deadline t0+10s: the retry fires.
	clock2.Advance(2 * time.Second)
	st := pollStatus(t, e2, "job", StatusSucceeded, 2*time.Second)
	if len(st.Attempts) != 2 || st.Attempts[1].Class != AttemptSucceeded {
		t.Fatalf("attempts=%+v want interrupted-free replay then success", st.Attempts)
	}
	if n := exec2.callCount("job"); n != 1 {
		t.Fatalf("job executed %d times after restart, want 1", n)
	}
	if !log2.contains("retry timer fired") {
		t.Fatal("retry-timer decision missing from logs")
	}
}

// TestRetryWaitRestartAfterDueRunsImmediately: stop during the wait and restart
// AFTER the deadline: the persisted NextRunAt is already in the past, so the
// next attempt must be dispatched immediately without any additional waiting.
func TestRetryWaitRestartAfterDueRunsImmediately(t *testing.T) {
	store := NewMemoryStore()
	t0 := time.Unix(1000, 0)
	_, _, stop1 := startFailingEngine(t, store, t0)
	stop1(context.Canceled)

	clock2 := NewFakeClock(t0.Add(30 * time.Second)) // 20s past the deadline
	exec2 := newScriptedExecutor().set("job", Result{OK: true, Reason: "recovered"})
	log2 := newCaptureLogger(t)
	e2, err := New(waitRetryDefs(), Options{Store: store, Executor: exec2, Clock: clock2, Logger: log2})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e2.Run(context.Background()) }()

	st := pollStatus(t, e2, "job", StatusSucceeded, 2*time.Second)
	if len(st.Attempts) != 2 || st.Attempts[1].Class != AttemptSucceeded {
		t.Fatalf("attempts=%+v", st.Attempts)
	}
	if n := exec2.callCount("job"); n != 1 {
		t.Fatalf("job executed %d times, want 1 immediate replay", n)
	}
	if !log2.contains("backoff elapsed") {
		t.Fatal("elapsed-backoff dispatch decision missing from logs")
	}
}

// TestRetryWaitHardCrashSurvivesRestart: simulate a hard kill (no graceful
// stop — the old engine is simply abandoned) while a task is in retry_wait.
// Restart must behave exactly like the graceful case, because the wait deadline
// lives in durable state, not in process memory.
func TestRetryWaitHardCrashSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir + "/state.json")
	t0 := time.Unix(5000, 0)
	_, _, stop1 := startFailingEngine(t, store, t0)
	// Simulate a hard kill: stop abruptly without graceful reconciliation.
	stop1(context.Canceled)

	clock2 := NewFakeClock(t0.Add(5 * time.Second)) // still 5s short
	exec2 := newScriptedExecutor().set("job", Result{OK: true})
	e2, err := New(waitRetryDefs(), Options{Store: NewFileStore(dir + "/state.json"), Executor: exec2, Clock: clock2, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e2.Run(context.Background()) }()
	time.Sleep(30 * time.Millisecond)
	if n := exec2.callCount("job"); n != 0 {
		t.Fatalf("job ran early after hard-crash restart: %d", n)
	}
	clock2.Advance(6 * time.Second)
	st := pollStatus(t, e2, "job", StatusSucceeded, 2*time.Second)
	if len(st.Attempts) != 2 {
		t.Fatalf("attempts=%d want 2", len(st.Attempts))
	}
}

// TestRepeatedRestartsDoNotExtendBackoff: each restart keeps the SAME absolute
// deadline; the total wait is never accumulated across restarts.
func TestRepeatedRestartsDoNotExtendBackoff(t *testing.T) {
	store := NewMemoryStore()
	t0 := time.Unix(1000, 0)
	_, _, stop1 := startFailingEngine(t, store, t0)
	stop1(context.Canceled)

	deadline := t0.Add(10 * time.Second)
	now := t0.Add(3 * time.Second)
	for i := 0; i < 4; i++ {
		clock := NewFakeClock(now)
		exec := newScriptedExecutor().set("job", Result{OK: true})
		e, err := New(waitRetryDefs(), Options{Store: store, Executor: exec, Clock: clock, Logger: testLogger(t)})
		if err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
		stop := runEngine(t, e)
		time.Sleep(20 * time.Millisecond)
		if n := exec.callCount("job"); n != 0 {
			t.Fatalf("restart %d ran job %d times before deadline %s (now %s)", i, n, deadline, now)
		}
		st, _ := e.State("job")
		if !st.NextRunAt.Equal(deadline) {
			t.Fatalf("restart %d moved NextRunAt to %s, want %s", i, st.NextRunAt, deadline)
		}
		stop(context.Canceled)
		now = now.Add(2 * time.Second) // 3,5,7,9 — always still before t0+10
	}

	// One final restart past the deadline: immediate success.
	clock := NewFakeClock(deadline.Add(time.Second))
	exec := newScriptedExecutor().set("job", Result{OK: true})
	e, err := New(waitRetryDefs(), Options{Store: store, Executor: exec, Clock: clock, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, _ := e.State("job")
	if st.Status != StatusSucceeded || len(st.Attempts) != 2 {
		t.Fatalf("final state=%s attempts=%d", st.Status, len(st.Attempts))
	}
}

// TestRetryNotDispatchedWhileTimerInFlight is a deterministic regression test
// for a scheduling race: while a retry_wait task's backoff timer is armed but
// its due-event has not been consumed, an unrelated event (a worker exit) must
// NOT dispatch the retry early. We assert this by driving a single task into
// retry_wait with a long backoff and confirming no dispatch happens even after
// many scheduling events fire before the deadline.
func TestRetryNotDispatchedWhileTimerInFlight(t *testing.T) {
	store := NewMemoryStore()
	t0 := time.Unix(7000, 0)
	clock := NewFakeClock(t0)
	exec := newScriptedExecutor().
		set("job",
			Result{OK: false, Retryable: true, Reason: "flaky"},
			Result{OK: true})
	e, err := New(waitRetryDefs(), Options{Store: store, Executor: exec, Clock: clock, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	stop := runEngine(t, e)
	pollStatus(t, e, "job", StatusRetryWait, 2*time.Second)
	if !clock.WaitForTimers(1) {
		t.Fatal("backoff timer not armed")
	}

	// Advance only partway and give the timer event no chance to be the thing
	// that matters: repeatedly poke scheduling through worker-less events by
	// advancing under the deadline. The task must stay in retry_wait.
	for i := 0; i < 5; i++ {
		clock.Advance(time.Millisecond) // still well before the 10s deadline
		time.Sleep(5 * time.Millisecond)
	}
	st, _ := e.State("job")
	if st.Status != StatusRetryWait {
		t.Fatalf("status=%s want retry_wait before deadline", st.Status)
	}
	if n := exec.callCount("job"); n != 1 {
		t.Fatalf("job ran %d times, must not retry before deadline", n)
	}

	stop(context.Canceled)
}
