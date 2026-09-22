package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Store persists snapshots durably. Implementations must guarantee that a
// committed snapshot is fully visible or the previous snapshot is intact
// (atomic replace), and must reject a Commit whose Version does not strictly
// exceed the version already durable (explicit state-rollback rejection).
type Store interface {
	Load() (*Snapshot, error)
	Commit(snap *Snapshot) error
}

// ErrNoSnapshot is returned by Load when nothing has ever been committed.
var ErrNoSnapshot = fs.ErrNotExist

// errVersionRollback marks a rejected non-monotonic commit.
type errVersionRollback struct {
	have int
	got  int
}

func (e *errVersionRollback) Error() string {
	return fmt.Sprintf("orchestrator: rejecting state rollback: committed version %d, incoming %d", e.have, e.got)
}

// FileStore is a JSON-file-backed Store. Writes are atomic: data is written
// to a temp file in the same directory, fsynced, then renamed over the target.
type FileStore struct {
	path string
	mu   sync.Mutex
	last int // highest durable version
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path, last: -1}
}

func (s *FileStore) Load() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNoSnapshot
		}
		return nil, &ValidationError{Reason: ReasonStoreUnavailable, Detail: err.Error()}
	}
	snap, err := decodeSnapshot(data)
	if err != nil {
		return nil, err
	}
	s.last = snap.Version
	return snap, nil
}

func (s *FileStore) Commit(snap *Snapshot) error {
	if snap == nil {
		return &ValidationError{Reason: ReasonInvalidState, Detail: "cannot commit nil snapshot"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Version <= s.last {
		return &errVersionRollback{have: s.last, got: snap.Version}
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return &ValidationError{Reason: ReasonInvalidState, Detail: "marshal snapshot: " + err.Error()}
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".orchestrator-*")
	if err != nil {
		return &ValidationError{Reason: ReasonStoreUnavailable, Detail: "create temp: " + err.Error()}
	}
	tmpName := tmp.Name()
	rollback := func(cause error) error {
		tmp.Close()
		os.Remove(tmpName)
		return cause
	}
	if _, err := tmp.Write(data); err != nil {
		return rollback(&ValidationError{Reason: ReasonStoreUnavailable, Detail: "write temp: " + err.Error()})
	}
	if err := tmp.Sync(); err != nil {
		return rollback(&ValidationError{Reason: ReasonStoreUnavailable, Detail: "fsync temp: " + err.Error()})
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return &ValidationError{Reason: ReasonStoreUnavailable, Detail: "close temp: " + err.Error()}
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return &ValidationError{Reason: ReasonStoreUnavailable, Detail: "rename: " + err.Error()}
	}
	s.last = snap.Version
	return nil
}

func decodeSnapshot(data []byte) (*Snapshot, error) {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, &ValidationError{Reason: ReasonInvalidState, Detail: "decode snapshot: " + err.Error()}
	}
	return &snap, nil
}

// MemoryStore is an in-process Store (useful for tests and embedders). It
// enforces the same version-monotonicity rule as FileStore.
type MemoryStore struct {
	mu     sync.Mutex
	snap   *Snapshot
	last   int
	loaded bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{last: -1}
}

func (s *MemoryStore) Load() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.loaded && s.snap == nil {
		return nil, ErrNoSnapshot
	}
	s.loaded = true
	if s.snap == nil {
		return nil, ErrNoSnapshot
	}
	s.last = s.snap.Version
	return cloneSnapshot(s.snap), nil
}

func (s *MemoryStore) Commit(snap *Snapshot) error {
	if snap == nil {
		return &ValidationError{Reason: ReasonInvalidState, Detail: "cannot commit nil snapshot"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Version <= s.last {
		return &errVersionRollback{have: s.last, got: snap.Version}
	}
	s.snap = cloneSnapshot(snap)
	s.last = snap.Version
	s.loaded = true
	return nil
}

func cloneSnapshot(s *Snapshot) *Snapshot {
	c := &Snapshot{Version: s.Version, Tasks: make(map[string]*TaskState, len(s.Tasks))}
	for id, st := range s.Tasks {
		cp := *st
		if st.Attempts != nil {
			cp.Attempts = append([]Attempt(nil), st.Attempts...)
		}
		c.Tasks[id] = &cp
	}
	return c
}
