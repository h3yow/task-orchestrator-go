package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// PlanMismatchError is returned by New when a persisted snapshot is internally
// intact but was produced under a different task plan (different task set,
// dependencies, timeout or retry policy). It is distinct from
// ValidationError(ReasonInvalidState), which means the state itself is
// corrupt/self-contradictory or rolled back.
type PlanMismatchError struct {
	StoredHash string
	WantedHash string
	// Details lists every detected difference, one per line.
	Details []string
}

func (e *PlanMismatchError) Error() string {
	var b strings.Builder
	b.WriteString("orchestrator: refusing to resume: persisted task plan differs from the definitions provided at startup")
	b.WriteString(fmt.Sprintf(" (stored plan hash %s, current plan hash %s)", shortHash(e.StoredHash), shortHash(e.WantedHash)))
	for _, d := range e.Details {
		b.WriteString("\n  - ")
		b.WriteString(d)
	}
	return b.String()
}

func shortHash(h string) string {
	if len(h) <= 12 {
		if h == "" {
			return "<none>"
		}
		return h
	}
	return h[:12]
}

// canonicalPlan returns the task definitions in a fixed (task-ID) order with
// dependency lists sorted, so the fingerprint is independent of declaration
// order or dependency listing order. Retry policies are normalized exactly as
// in New, so equivalent policies hash identically.
func canonicalPlan(order []string, defs map[string]TaskDef) []TaskDef {
	ids := append([]string(nil), order...)
	sort.Strings(ids)
	out := make([]TaskDef, 0, len(ids))
	for _, id := range ids {
		d := defs[id]
		d.Deps = append([]string(nil), d.Deps...)
		sort.Strings(d.Deps)
		d.Retry = d.Retry.normalized()
		out = append(out, d)
	}
	return out
}

// planFingerprint returns the SHA-256 hex digest of the canonical plan. Only
// fields that affect execution participate: ID, dependency set, per-attempt
// timeout and the (normalized) retry policy.
func planFingerprint(plan []TaskDef) (string, error) {
	type fpRetry struct {
		MaxAttempts    int   `json:"max_attempts"`
		InitialBackoff int64 `json:"initial_backoff_ns"`
		MaxBackoff     int64 `json:"max_backoff_ns"`
	}
	type fpTask struct {
		ID      string   `json:"id"`
		Deps    []string `json:"deps"`
		Timeout int64    `json:"timeout_ns"`
		Retry   fpRetry  `json:"retry"`
	}
	tasks := make([]fpTask, 0, len(plan))
	for _, d := range plan {
		r := d.Retry.normalized()
		tasks = append(tasks, fpTask{
			ID:      d.ID,
			Deps:    append([]string(nil), d.Deps...),
			Timeout: int64(d.Timeout),
			Retry: fpRetry{
				MaxAttempts:    r.MaxAttempts,
				InitialBackoff: int64(r.InitialBackoff),
				MaxBackoff:     int64(r.MaxBackoff),
			},
		})
	}
	data, err := json.Marshal(tasks)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// verifyPlanMatch rejects snapshots whose stored plan differs from the plan the
// engine was constructed with. The verdict order is:
//
//  1. snapshot with no stored fingerprint -> plan mismatch (state predates plan
//     tracking; cannot prove the plan is the same, so resume is refused);
//  2. fingerprint equals the current plan's -> accepted, regardless of whether
//     the embedded plan copy is present (it is only diagnostic);
//  3. otherwise -> plan mismatch, and the embedded plan is diffed against the
//     current plan to enumerate concrete differences.
//
// Self-contradictory snapshot CONTENT is handled separately by
// validateSnapshot (ReasonInvalidState); this function judges only plan
// identity, which is why the cause type is PlanMismatchError.
func verifyPlanMatch(snap *Snapshot, plan []TaskDef, hash string) error {
	if snap.PlanHash == "" {
		return &PlanMismatchError{
			StoredHash: "",
			WantedHash: hash,
			Details:    []string{"persisted snapshot carries no plan fingerprint, so plan identity cannot be verified"},
		}
	}
	if snap.PlanHash == hash {
		return nil
	}
	return &PlanMismatchError{
		StoredHash: snap.PlanHash,
		WantedHash: hash,
		Details:    diffPlans(snap.Plan, plan),
	}
}

// diffPlans renders human-readable differences between the stored and current
// canonical plans. Missing / added tasks are reported first (IDs sorted), then
// per-field changes for tasks present in both (IDs sorted).
func diffPlans(stored, current []TaskDef) []string {
	old := make(map[string]TaskDef, len(stored))
	for _, d := range stored {
		old[d.ID] = d
	}
	want := make(map[string]TaskDef, len(current))
	for _, d := range current {
		want[d.ID] = d
	}

	var details []string
	var added, removed []string
	for id := range want {
		if _, ok := old[id]; !ok {
			added = append(added, id)
		}
	}
	for id := range old {
		if _, ok := want[id]; !ok {
			removed = append(removed, id)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	for _, id := range added {
		details = append(details, fmt.Sprintf("task %q exists in the current plan but not in the persisted plan (task added)", id))
	}
	for _, id := range removed {
		details = append(details, fmt.Sprintf("task %q exists in the persisted plan but not in the current plan (task removed)", id))
	}

	common := make([]string, 0)
	for id := range want {
		if _, ok := old[id]; ok {
			common = append(common, id)
		}
	}
	sort.Strings(common)
	for _, id := range common {
		o := old[id]
		w := want[id]
		if !sameDeps(o.Deps, w.Deps) {
			details = append(details, fmt.Sprintf("task %q dependencies changed: was %s, now %s",
				id, fmtDeps(o.Deps), fmtDeps(w.Deps)))
		}
		if o.Timeout != w.Timeout {
			details = append(details, fmt.Sprintf("task %q timeout changed: was %s, now %s",
				id, fmtDur(o.Timeout), fmtDur(w.Timeout)))
		}
		if ro, rw := o.Retry.normalized(), w.Retry.normalized(); ro != rw {
			if ro.MaxAttempts != rw.MaxAttempts {
				details = append(details, fmt.Sprintf("task %q retry budget changed: was %d attempt(s), now %d",
					id, ro.MaxAttempts, rw.MaxAttempts))
			}
			if ro.InitialBackoff != rw.InitialBackoff {
				details = append(details, fmt.Sprintf("task %q initial backoff changed: was %s, now %s",
					id, ro.InitialBackoff, rw.InitialBackoff))
			}
			if ro.MaxBackoff != rw.MaxBackoff {
				details = append(details, fmt.Sprintf("task %q max backoff changed: was %s, now %s",
					id, ro.MaxBackoff, rw.MaxBackoff))
			}
		}
	}
	if len(details) == 0 {
		// Can happen for a pre-fingerprint snapshot whose embedded plan copy is
		// missing: the hashes differ but no structured diff is available.
		details = append(details, "stored plan details are unavailable; hashes differ")
	}
	return details
}

func sameDeps(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	xs := append([]string(nil), a...)
	ys := append([]string(nil), b...)
	sort.Strings(xs)
	sort.Strings(ys)
	for i := range xs {
		if xs[i] != ys[i] {
			return false
		}
	}
	return true
}

func fmtDeps(deps []string) string {
	if len(deps) == 0 {
		return "[]"
	}
	cp := append([]string(nil), deps...)
	sort.Strings(cp)
	quoted := make([]string, len(cp))
	for i, d := range cp {
		quoted[i] = quote(d)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "0 (no timeout)"
	}
	return d.String()
}
