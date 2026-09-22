package orchestrator

import (
	"context"
	"testing"
	"time"
)

func TestSmokeDiamondSuccess(t *testing.T) {
	exec := &countingExecutor{results: map[string]Result{
		"a": {OK: true},
		"b": {OK: true},
		"c": {OK: true},
		"d": {OK: true},
	}}
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"a"}},
		{ID: "d", Deps: []string{"b", "c"}},
	}
	e, err := New(defs, Options{
		Store:    store,
		Executor: exec,
		Clock:    NewFakeClock(time.Unix(1000, 0)),
		Logger:   testLogger(t),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		st, _ := e.State(id)
		if st.Status != StatusSucceeded {
			t.Fatalf("task %s status=%s, want succeeded", id, st.Status)
		}
	}
	snap := e.Snapshot()
	if snap.Version < 5 {
		t.Fatalf("version=%d, expected >=5", snap.Version)
	}
}
