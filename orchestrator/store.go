package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Store persists run snapshots. Save must be durable before it returns;
// the engine relies on persistence boundaries for restart recovery.
type Store interface {
	Load(ctx context.Context, runID string) (*Snapshot, error)
	Save(ctx context.Context, snap *Snapshot) error
}

// ---------------------------------------------------------------- FileStore

// FileStore stores each run as an atomic JSON snapshot:
// <dir>/<runID>.json. Writes go to a temp file in the same directory and
// are renamed into place, so a crash can only leave the old snapshot or
// the new one, never a torn write.
type FileStore struct {
	dir string
}

func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty store directory", ErrStoreFault)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStoreFault, err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(runID string) (string, error) {
	if runID == "" || strings.ContainsAny(runID, `/\`) || runID == "." || runID == ".." {
		return "", fmt.Errorf("%w: unsafe run id %q", ErrStoreFault, runID)
	}
	return filepath.Join(s.dir, runID+".json"), nil
}

func (s *FileStore) Load(ctx context.Context, runID string) (*Snapshot, error) {
	p, err := s.path(runID)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStoreFault, err)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		// Corrupt bytes are an explicit, non-recoverable fault.
		return nil, fmt.Errorf("%w: snapshot for %q is corrupt: %v",
			ErrInvalidState, runID, err)
	}
	return &snap, nil
}

func (s *FileStore) Save(ctx context.Context, snap *Snapshot) error {
	if snap == nil {
		return fmt.Errorf("%w: refusing to persist nil snapshot", ErrInvalidState)
	}
	p, err := s.path(snap.RunID)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStoreFault, err)
	}
	tmp, err := os.CreateTemp(s.dir, ".snap-*.tmp")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStoreFault, err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("%w: %v", ErrStoreFault, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("%w: %v", ErrStoreFault, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("%w: %v", ErrStoreFault, err)
	}
	if err := os.Rename(tmpName, p); err != nil {
		return fmt.Errorf("%w: %v", ErrStoreFault, err)
	}
	cleanup = false
	syncDir(s.dir) // best-effort durability of the rename
	return nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// ---------------------------------------------------------------- MemoryStore

// MemoryStore keeps the latest snapshot per run in memory. It is safe for
// concurrent use and intended for tests and embeddings.
type MemoryStore struct {
	mu   sync.Mutex
	data map[string]*Snapshot
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: map[string]*Snapshot{}}
}

func (s *MemoryStore) Load(ctx context.Context, runID string) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.data[runID]
	if !ok {
		return nil, nil
	}
	return cloneSnapshot(snap), nil
}

func (s *MemoryStore) Save(ctx context.Context, snap *Snapshot) error {
	if snap == nil {
		return fmt.Errorf("%w: refusing to persist nil snapshot", ErrInvalidState)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[snap.RunID] = cloneSnapshot(snap)
	return nil
}

func cloneSnapshot(snap *Snapshot) *Snapshot {
	b, err := json.Marshal(snap)
	if err != nil {
		panic(err)
	}
	var out Snapshot
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return &out
}

// --------------------------------------------------------- snapshot validity

var validStatuses = map[Status]bool{
	StatusPending: true, StatusReady: true, StatusRunning: true,
	StatusRetrying: true, StatusSucceeded: true, StatusFailed: true,
	StatusSkipped: true,
}

// validateSnapshot rejects any snapshot that is structurally broken,
// belongs to a different plan, violates per-task invariants or contradicts
// the dependency graph. The engine never advances from such a state.
func validateSnapshot(snap *Snapshot, plan *Plan) error {
	if snap == nil {
		return fmt.Errorf("%w: snapshot is nil", ErrInvalidState)
	}
	if plan != nil {
		if snap.RunID != plan.RunID() {
			return fmt.Errorf("%w: run id %q vs plan %q",
				ErrPlanMismatch, snap.RunID, plan.RunID())
		}
		if snap.PlanHash != plan.Hash() {
			return fmt.Errorf("%w: run %q graph/retry configuration changed since it was persisted",
				ErrPlanMismatch, snap.RunID)
		}
	}
	if len(snap.Tasks) != len(plan.Tasks()) {
		return fmt.Errorf("%w: run %q has %d persisted tasks but plan has %d",
			ErrInvalidState, snap.RunID, len(snap.Tasks), len(plan.Tasks()))
	}
	for _, id := range plan.IDs() {
		st, ok := snap.Tasks[id]
		if !ok {
			return fmt.Errorf("%w: missing state for task %q", ErrInvalidState, id)
		}
		if st.ID != id {
			return fmt.Errorf("%w: task map key %q does not match state id %q",
				ErrInvalidState, id, st.ID)
		}
		if err := validateTaskState(st, plan, snap); err != nil {
			return err
		}
	}
	for id := range snap.Tasks {
		if _, ok := plan.Task(id); !ok {
			return fmt.Errorf("%w: persisted unknown task %q", ErrInvalidState, id)
		}
	}
	return nil
}

func validateTaskState(st *TaskState, plan *Plan, snap *Snapshot) error {
	id := st.ID
	def, _ := plan.Task(id)
	if !validStatuses[st.Status] {
		return fmt.Errorf("%w: task %q has unknown status %q",
			ErrInvalidState, id, st.Status)
	}
	if st.Attempt < 0 {
		return fmt.Errorf("%w: task %q has negative attempt counter %d",
			ErrInvalidState, id, st.Attempt)
	}
	if st.Attempt < len(st.Records) {
		return fmt.Errorf("%w: task %q regressed: attempts started %d < records %d",
			ErrInvalidState, id, st.Attempt, len(st.Records))
	}
	for i, r := range st.Records {
		if r.Attempt != i+1 {
			return fmt.Errorf("%w: task %q record %d has attempt number %d",
				ErrInvalidState, id, i, r.Attempt)
		}
	}

	switch st.Status {
	case StatusPending:
		if st.Attempt != 0 || len(st.Records) != 0 {
			return fmt.Errorf("%w: task %q is %s but has attempt history",
				ErrInvalidState, id, st.Status)
		}
	case StatusReady:
		// Ready normally precedes the first attempt, but after crash
		// recovery an interrupted/retrying task is also re-homed to ready;
		// in that case attempt == len(records).
		if st.Attempt != len(st.Records) {
			return fmt.Errorf("%w: task %q ready with inconsistent attempt count %d/%d",
				ErrInvalidState, id, st.Attempt, len(st.Records))
		}
	case StatusRunning:
		// An in-flight attempt has started but has no terminal record yet.
		if st.Attempt < 1 || st.Attempt != len(st.Records)+1 {
			return fmt.Errorf("%w: task %q is running with inconsistent attempt count %d/%d",
				ErrInvalidState, id, st.Attempt, len(st.Records))
		}
	case StatusRetrying:
		if st.Attempt < 1 || st.Attempt != len(st.Records) {
			return fmt.Errorf("%w: task %q is retrying with inconsistent attempt count %d/%d",
				ErrInvalidState, id, st.Attempt, len(st.Records))
		}
		if st.LastReason == ReasonNone {
			return fmt.Errorf("%w: task %q is retrying without a recorded failure reason",
				ErrInvalidState, id)
		}
	case StatusSucceeded:
		if st.Attempt < 1 || st.Attempt != len(st.Records) {
			return fmt.Errorf("%w: succeeded task %q has inconsistent attempt count",
				ErrInvalidState, id)
		}
		last := st.Records[len(st.Records)-1]
		if last.Err != "" || last.Reason != ReasonNone {
			return fmt.Errorf("%w: succeeded task %q has a failing final record",
				ErrInvalidState, id)
		}
	case StatusFailed:
		if st.Attempt < 1 || st.Attempt != len(st.Records) {
			return fmt.Errorf("%w: failed task %q has inconsistent attempt count",
				ErrInvalidState, id)
		}
		if st.LastReason == ReasonNone {
			return fmt.Errorf("%w: failed task %q has no terminal failure reason",
				ErrInvalidState, id)
		}
	case StatusSkipped:
		if st.LastReason != ReasonDependencyFailed {
			return fmt.Errorf("%w: skipped task %q has reason %q",
				ErrInvalidState, id, st.LastReason)
		}
	}

	// Cross-task graph invariants.
	depDone := 0
	depDead := 0
	for _, dep := range def.Deps {
		dst := snap.Tasks[dep]
		switch dst.Status {
		case StatusSucceeded:
			depDone++
		case StatusFailed, StatusSkipped:
			depDead++
		}
	}
	switch st.Status {
	case StatusReady, StatusRunning, StatusRetrying, StatusSucceeded:
		if depDone != len(def.Deps) {
			return fmt.Errorf("%w: task %q is %s while not every dependency succeeded (%d/%d)",
				ErrInvalidState, id, st.Status, depDone, len(def.Deps))
		}
	case StatusSkipped:
		if depDead == 0 {
			return fmt.Errorf("%w: task %q is skipped but no dependency failed or was skipped",
				ErrInvalidState, id)
		}
	}
	return nil
}
