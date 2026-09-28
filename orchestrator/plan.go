package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// planFingerprint is the canonical, order-independent content of one task
// definition used by planDigest.
type planFingerprint struct {
	ID                  string   `json:"id"`
	Deps                []string `json:"deps"`
	TimeoutNanos        int64    `json:"timeout_nanos"`
	MaxAttempts         int      `json:"max_attempts"`
	InitialBackoffNanos int64    `json:"initial_backoff_nanos"`
	MaxBackoffNanos     int64    `json:"max_backoff_nanos"`
}

// planDigest returns the stable SHA-256 fingerprint of a complete task plan.
// It depends only on plan content, never on declaration order: task IDs are
// sorted and each task's dependency list is sorted before hashing. The
// supplied defs must already be normalized (RetryPolicy.normalized).
func planDigest(defs map[string]TaskDef, order []string) (string, error) {
	ids := append([]string(nil), order...)
	sort.Strings(ids)
	fps := make([]planFingerprint, 0, len(ids))
	for _, id := range ids {
		d := defs[id]
		deps := append([]string(nil), d.Deps...)
		sort.Strings(deps)
		fps = append(fps, planFingerprint{
			ID:                  d.ID,
			Deps:                deps,
			TimeoutNanos:        int64(d.Timeout),
			MaxAttempts:         d.Retry.MaxAttempts,
			InitialBackoffNanos: int64(d.Retry.InitialBackoff),
			MaxBackoffNanos:     int64(d.Retry.MaxBackoff),
		})
	}
	data, err := json.Marshal(fps)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// checkPlan decides whether a loaded snapshot belongs to the supplied plan.
//
// Return values distinguish two fundamentally different restart failures:
//   - ReasonPlanChanged: the snapshot is internally fine, but the definitions
//     supplied at this start do not match the ones the state was produced with
//     (task added/removed, or any dependency/timeout/retry change);
//   - ReasonInvalidState: the snapshot's own plan evidence is contradictory or
//     its task set is structurally broken (state corruption / rollback), even
//     though the definitions on their own are valid.
func checkPlan(snap *Snapshot, defs map[string]TaskDef, digest string) error {
	if snap == nil {
		return &ValidationError{Reason: ReasonInvalidState, Detail: "snapshot is nil"}
	}
	// Structural task-set check. A digest only certifies content for the tasks
	// it was computed over, so the set itself must match first.
	if len(snap.Tasks) != len(defs) {
		return planOrStateError(snap, digest, fmt.Sprintf(
			"task set changed: persisted state has %d tasks, current plan defines %d",
			len(snap.Tasks), len(defs)))
	}
	for id := range defs {
		if _, ok := snap.Tasks[id]; !ok {
			return planOrStateError(snap, digest,
				"task "+quote(id)+" is defined now but missing from persisted state")
		}
	}
	for id := range snap.Tasks {
		if _, ok := defs[id]; !ok {
			return planOrStateError(snap, digest,
				"persisted state contains task "+quote(id)+" that is no longer defined")
		}
	}

	// The task sets match. Any mismatch now is a content change.
	if snap.PlanDigest == "" {
		// Every snapshot this engine commits carries a digest. A loaded
		// snapshot without one therefore cannot be a valid state for this
		// plan: the evidence of plan identity has been stripped (corruption /
		// hand-editing), so resume is refused rather than guessing.
		return &ValidationError{
			Reason: ReasonInvalidState,
			Detail: "persisted snapshot is missing its plan digest (state corrupted or written by an incompatible version); refusing to resume",
		}
	}
	if snap.PlanDigest != digest {
		return &ValidationError{
			Reason: ReasonPlanChanged,
			Detail: "task definitions differ from the plan the persisted state was produced with " +
				"(dependency, timeout or retry policy changed); refusing to resume: " +
				"persisted digest " + snap.PlanDigest + ", current digest " + digest,
		}
	}
	return nil
}

// planOrStateError classifies a task-set mismatch. When the snapshot carries a
// digest that matches the current definitions, the mismatch cannot be caused by
// a plan change (the plan is what was persisted): it must be state corruption
// or rollback. Otherwise it is a plan change.
func planOrStateError(snap *Snapshot, digest, detail string) error {
	if snap.PlanDigest != "" && snap.PlanDigest == digest {
		return &ValidationError{
			Reason: ReasonInvalidState,
			Detail: "snapshot task set contradicts its own plan digest (state corrupted or rolled back): " + detail,
		}
	}
	return &ValidationError{Reason: ReasonPlanChanged, Detail: detail}
}

// shortDigest trims a digest for log readability.
func shortDigest(d string) string {
	if len(d) > 12 {
		return d[:12]
	}
	return d
}
