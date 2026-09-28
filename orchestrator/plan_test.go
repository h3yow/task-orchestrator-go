package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func basePlan() []TaskDef {
	return []TaskDef{
		{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 5 * time.Second, MaxBackoff: time.Minute}},
		{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}
}

// primeStore runs one engine to completion and returns its persisted store.
func primeStore(t *testing.T, defs []TaskDef) Store {
	t.Helper()
	store := NewMemoryStore()
	exec := &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}}
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store
}

// TestRestartRejectsChangedPlan covers every kind of plan edit: each must be
// refused with *PlanMismatchError (Reason plan_mismatch), never silently
// resumed, and the error must name what changed.
func TestRestartRejectsChangedPlan(t *testing.T) {
	cases := map[string][]TaskDef{
		"task added": {
			{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 5 * time.Second, MaxBackoff: time.Minute}},
			{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
			{ID: "c"},
		},
		"task removed": {
			{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 5 * time.Second, MaxBackoff: time.Minute}},
		},
		"dependency changed": {
			{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 5 * time.Second, MaxBackoff: time.Minute}},
			// b used to depend on a; now it does not
			{ID: "b", Retry: RetryPolicy{MaxAttempts: 1}},
		},
		"timeout changed": {
			{ID: "a", Timeout: 2 * time.Second, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 5 * time.Second, MaxBackoff: time.Minute}},
			{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
		},
		"retry budget changed": {
			{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 5, InitialBackoff: 5 * time.Second, MaxBackoff: time.Minute}},
			{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
		},
		"backoff changed": {
			{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 9 * time.Second, MaxBackoff: time.Minute}},
			{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
		},
		"max backoff changed": {
			{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 5 * time.Second, MaxBackoff: 30 * time.Second}},
			{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
		},
	}

	for name, changed := range cases {
		t.Run(name, func(t *testing.T) {
			store := primeStore(t, basePlan())
			_, err := New(changed, Options{
				Store:    store,
				Executor: &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}},
				Clock:    NewFakeClock(time.Unix(100, 0)),
				Logger:   testLogger(t),
			})
			var pm *PlanMismatchError
			if !errors.As(err, &pm) {
				t.Fatalf("err=%v, want *PlanMismatchError", err)
			}
			if pm.StoredHash == "" || pm.WantedHash == "" {
				t.Fatalf("mismatch error missing hashes: %+v", pm)
			}
			if pm.StoredHash == pm.WantedHash {
				t.Fatalf("mismatch reported with equal hashes %q", pm.StoredHash)
			}
			if len(pm.Details) == 0 {
				t.Fatalf("mismatch error explains no differences: %v", pm)
			}
		})
	}
}

// TestPlanMismatchDistinctFromCorruptState: a changed plan yields
// PlanMismatchError while corrupted state under the SAME plan yields
// ValidationError(ReasonInvalidState); callers must be able to tell the two
// failure classes apart.
func TestPlanMismatchDistinctFromCorruptState(t *testing.T) {
	// 1. Plan changed -> PlanMismatchError.
	store := primeStore(t, basePlan())
	_, errPlan := New([]TaskDef{{ID: "a"}, {ID: "b"}}, Options{
		Store: store, Executor: &countingExecutor{},
		Clock: NewFakeClock(time.Unix(1, 0)),
	})
	var pm *PlanMismatchError
	if !errors.As(errPlan, &pm) {
		t.Fatalf("changed plan err=%v, want *PlanMismatchError", errPlan)
	}
	var ve *ValidationError
	if errors.As(errPlan, &ve) {
		t.Fatalf("changed plan must not be classified as invalid state: %v", errPlan)
	}

	// 2. Same plan, corrupted snapshot -> ValidationError(invalid_persisted_state).
	dir := t.TempDir()
	path := dir + "/state.json"
	fs := NewFileStore(path)
	e, err := New(basePlan(), Options{
		Store: fs, Executor: &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}},
		Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	good, rerr := readFile(path)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if err := writeFile(path, jsonReplaceStatus(good, "succeeded", "pending")); err != nil {
		t.Fatal(err)
	}
	_, errCorrupt := New(basePlan(), Options{
		Store: fs, Executor: &countingExecutor{},
		Clock: NewFakeClock(time.Unix(1, 0)),
	})
	if !errors.As(errCorrupt, &ve) || ve.Reason != ReasonInvalidState {
		t.Fatalf("corrupt state err=%v, want ValidationError(%s)", errCorrupt, ReasonInvalidState)
	}
	if errors.As(errCorrupt, &pm) {
		t.Fatal("corrupt state under the same plan must not be reported as a plan mismatch")
	}
}

// TestSamePlanResumes: declaration order and dependency-listing order are
// irrelevant to plan identity: an equivalent plan resumes without error, and
// completed tasks are never re-run.
func TestSamePlanResumes(t *testing.T) {
	store := primeStore(t, basePlan())
	// Same definitions, reordered: b before a; task b's retry policy is given
	// in zero-value form that normalizes to the same policy the stored plan was
	// canonicalized with.
	equiv := []TaskDef{
		{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
		{ID: "a", Timeout: time.Second, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 5 * time.Second, MaxBackoff: time.Minute}},
	}
	exec := &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}}
	e2, err := New(equiv, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(50, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatalf("equivalent plan rejected: %v", err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec.callCount("a") != 0 || exec.callCount("b") != 0 {
		t.Fatalf("tasks re-executed under equivalent plan: a=%d b=%d", exec.callCount("a"), exec.callCount("b"))
	}
}

// TestMismatchDetailsAreSpecific: the diagnostic text names the exact task and
// field, so an operator can see what changed without comparing JSON by hand.
func TestMismatchDetailsAreSpecific(t *testing.T) {
	store := primeStore(t, basePlan())
	_, err := New([]TaskDef{
		{ID: "a", Timeout: 7 * time.Second, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 5 * time.Second, MaxBackoff: time.Minute}},
		{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}, Options{Store: store, Executor: &countingExecutor{}, Clock: NewFakeClock(time.Unix(2, 0))})
	var pm *PlanMismatchError
	if !errors.As(err, &pm) {
		t.Fatalf("err=%v", err)
	}
	joined := strings.Join(pm.Details, "\n")
	for _, want := range []string{`"a"`, "timeout"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("details %q do not mention %s", joined, want)
		}
	}
}

// TestLegacySnapshotWithoutFingerprintRejected: a snapshot written before plan
// fingerprints existed cannot prove plan identity and is refused as a plan
// mismatch (not accepted, and not mislabeled as corrupt state).
func TestLegacySnapshotWithoutFingerprintRejected(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	snap := &Snapshot{
		Version: 0,
		Tasks: map[string]*TaskState{
			"a": {Status: StatusPending, UpdatedAt: time.Unix(0, 0)},
		},
		// PlanHash intentionally absent.
	}
	store := NewFileStore(path)
	if err := store.Commit(snap); err != nil {
		t.Fatal(err)
	}
	_, err := New([]TaskDef{{ID: "a"}}, Options{
		Store: store, Executor: &countingExecutor{}, Clock: NewFakeClock(time.Unix(1, 0)),
	})
	var pm *PlanMismatchError
	if !errors.As(err, &pm) {
		t.Fatalf("err=%v, want PlanMismatchError for fingerprint-less snapshot", err)
	}
}
