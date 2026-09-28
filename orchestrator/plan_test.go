package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

func basePlan() []TaskDef {
	return []TaskDef{
		{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, MaxBackoff: time.Second}},
		{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
		{ID: "c", Deps: []string{"a", "b"}, Retry: RetryPolicy{MaxAttempts: 2}},
	}
}

// copyDefs deep-copies defs so a test mutating Deps cannot share backing
// arrays with another subtest's plan.
func copyDefs(in []TaskDef) []TaskDef {
	out := make([]TaskDef, len(in))
	for i, d := range in {
		out[i] = d
		if d.Deps != nil {
			out[i].Deps = append([]string(nil), d.Deps...)
		}
	}
	return out
}

func digestFor(t *testing.T, defs []TaskDef) string {
	t.Helper()
	index, order := normalizeDefs(defs)
	d, err := planDigest(index, order)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func normalizeDefs(defs []TaskDef) (map[string]TaskDef, []string) {
	index := make(map[string]TaskDef, len(defs))
	order := make([]string, 0, len(defs))
	for _, d := range defs {
		n := d
		n.Retry = d.Retry.normalized()
		index[d.ID] = n
		order = append(order, d.ID)
	}
	return index, order
}

// TestPlanDigestStableAndOrderIndependent: same content in a different
// declaration order or dep order must produce the same digest.
func TestPlanDigestStableAndOrderIndependent(t *testing.T) {
	d1 := digestFor(t, basePlan())

	reordered := []TaskDef{
		{ID: "c", Deps: []string{"b", "a"}, Retry: RetryPolicy{MaxAttempts: 2}},
		{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, MaxBackoff: time.Second}},
		{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}
	if d2 := digestFor(t, reordered); d2 != d1 {
		t.Fatalf("digest changed with order:\n%s\n%s", d1, d2)
	}

	// Sanity: the digest is actually content-sensitive.
	changedDeps := copyDefs(basePlan())
	changedDeps[2].Deps = []string{"b"} // c no longer waits for a directly
	if d3 := digestFor(t, changedDeps); d3 == d1 {
		t.Fatal("digest identical after dependency change")
	}
	changedTimeout := copyDefs(basePlan())
	changedTimeout[0].Timeout = 2 * time.Second
	if d3 := digestFor(t, changedTimeout); d3 == d1 {
		t.Fatal("digest identical after timeout change")
	}
	changedRetry := copyDefs(basePlan())
	changedRetry[0].Retry.MaxAttempts = 5
	if d3 := digestFor(t, changedRetry); d3 == d1 {
		t.Fatal("digest identical after retry policy change")
	}
	changedBackoff := copyDefs(basePlan())
	changedBackoff[0].Retry.InitialBackoff = 20 * time.Millisecond
	if d3 := digestFor(t, changedBackoff); d3 == d1 {
		t.Fatal("digest identical after backoff change")
	}
}

// runToRetryWait starts an engine, lets "a" fail once into retry_wait with a
// long backoff, then returns the store and the clock's base time.
func runToRetryWait(t *testing.T, defs []TaskDef) (*MemoryStore, time.Time) {
	t.Helper()
	store := NewMemoryStore()
	exec := newScriptedExecutor().
		set("a", Result{OK: false, Retryable: true, Reason: "flaky"}, Result{OK: true}).
		set("b", Result{OK: true}).
		set("c", Result{OK: true})
	e, err := New(defs, Options{
		Store: store, Executor: exec,
		Clock:  NewFakeClock(time.Unix(1000, 0)),
		Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "a", StatusRetryWait, 2*time.Second)
	return store, time.Unix(1000, 0)
}

// TestRestartRejectsChangedPlan is the headline plan-consistency case: state
// produced under one plan cannot be resumed with a different one.
func TestRestartRejectsChangedPlan(t *testing.T) {
	cases := map[string]func([]TaskDef) []TaskDef{
		"dependency changed": func(d []TaskDef) []TaskDef {
			d[2].Deps = []string{"b"}
			return d
		},
		"dependency added": func(d []TaskDef) []TaskDef {
			d[1].Deps = []string{"a"}
			d[2].Deps = []string{"a", "b"}
			d = append(d, TaskDef{ID: "d", Deps: []string{"c"}})
			return d
		},
		"task removed": func(d []TaskDef) []TaskDef {
			return d[:2]
		},
		"timeout changed": func(d []TaskDef) []TaskDef {
			d[0].Timeout = 5 * time.Second
			return d
		},
		"retry policy changed": func(d []TaskDef) []TaskDef {
			d[0].Retry.MaxAttempts = 9
			return d
		},
		"backoff changed": func(d []TaskDef) []TaskDef {
			d[0].Retry.InitialBackoff = time.Hour
			return d
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			store, _ := runToRetryWait(t, basePlan())
			changed := mutate(copyDefs(basePlan()))
			_, err := New(changed, Options{
				Store:    store,
				Executor: newScriptedExecutor(),
				Clock:    NewFakeClock(time.Unix(2000, 0)),
				Logger:   testLogger(t),
			})
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("err=%v, want *ValidationError", err)
			}
			if ve.Reason != ReasonPlanChanged {
				t.Fatalf("reason=%s, want %s (detail=%s)", ve.Reason, ReasonPlanChanged, ve.Detail)
			}
		})
	}
}

// TestRestartAcceptsIdenticalPlan: same definitions (even reordered, even with
// a dep listed in different order) resume normally.
func TestRestartAcceptsIdenticalPlan(t *testing.T) {
	store, _ := runToRetryWait(t, basePlan())
	reordered := []TaskDef{
		{ID: "c", Deps: []string{"b", "a"}, Retry: RetryPolicy{MaxAttempts: 2}},
		{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, MaxBackoff: time.Second}},
		{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}
	exec := newScriptedExecutor().
		set("a", Result{OK: true}).
		set("b", Result{OK: true}).
		set("c", Result{OK: true})
	e, err := New(reordered, Options{
		Store: store, Executor: exec,
		Clock:  NewFakeClock(time.Unix(2000, 0)),
		Logger: testLogger(t),
	})
	if err != nil {
		t.Fatalf("identical plan rejected: %v", err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		st, _ := e.State(id)
		if st.Status != StatusSucceeded {
			t.Fatalf("%s=%s want succeeded", id, st.Status)
		}
	}
}

// TestCorruptTaskSetWithMatchingDigestIsInvalidState: when the snapshot's own
// digest matches the current definitions but its task set contradicts that
// digest, the cause is corruption/rollback — not a plan change.
func TestCorruptTaskSetWithMatchingDigestIsInvalidState(t *testing.T) {
	defs := []TaskDef{{ID: "a"}, {ID: "b"}}
	store := NewMemoryStore()
	exec := &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}}
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Tamper with the durable state: delete a task row but leave the digest in
	// place. The definitions are unchanged, so this is state corruption.
	store.mu.Lock()
	if store.snap.PlanDigest == "" {
		store.mu.Unlock()
		t.Fatal("fresh snapshot should carry a plan digest")
	}
	delete(store.snap.Tasks, "b")
	store.mu.Unlock()

	_, err = New(defs, Options{
		Store:    store,
		Executor: exec,
		Clock:    NewFakeClock(time.Unix(100, 0)),
		Logger:   testLogger(t),
	})
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err=%v want *ValidationError", err)
	}
	if ve.Reason != ReasonInvalidState {
		t.Fatalf("reason=%s want %s: %v", ve.Reason, ReasonInvalidState, err)
	}
}

// TestMissingDigestIsInvalidState: stripping the plan digest from an otherwise
// intact snapshot must be refused as corrupted state, not silently resumed.
func TestMissingDigestIsInvalidState(t *testing.T) {
	defs := []TaskDef{{ID: "a"}, {ID: "b"}}
	store := NewMemoryStore()
	exec := &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}}
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.snap.PlanDigest = ""
	store.mu.Unlock()
	_, err = New(defs, Options{
		Store: store, Executor: exec,
		Clock: NewFakeClock(time.Unix(1, 0)), Logger: testLogger(t),
	})
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Reason != ReasonInvalidState {
		t.Fatalf("err=%v want ValidationError(%s)", err, ReasonInvalidState)
	}
}

// TestTamperedStateContentIsInvalidState: a hand-corrupted snapshot (status
// rewritten) reports invalid_persisted_state, never plan_changed.
func TestTamperedStateContentIsInvalidState(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	store := NewFileStore(path)
	defs := []TaskDef{{ID: "a"}}
	exec := &countingExecutor{results: map[string]Result{"a": {OK: true}}}
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	good, err := readFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFile(path, jsonReplaceStatus(good, "succeeded", "running")); err != nil {
		t.Fatal(err)
	}
	_, err = New(defs, Options{Store: NewFileStore(path), Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Reason != ReasonInvalidState {
		t.Fatalf("err=%v want ValidationError(%s)", err, ReasonInvalidState)
	}
}
