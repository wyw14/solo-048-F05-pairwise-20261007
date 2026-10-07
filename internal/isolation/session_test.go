package isolation

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/network"
	"heatpulse/internal/store"
	"heatpulse/internal/valve"
)

func testHarness(t *testing.T, dir string, ttl time.Duration, reliefDelay time.Duration) (*Service, *valve.Registry, *valve.ReliefService, *valve.BoundaryService, *network.State) {
	t.Helper()
	state := network.NewState([]network.Zone{{ID: "east", Name: "East", Phase: network.PhaseActive}})
	topology := network.NewTopology([]network.Section{{
		ID: "east-branch", ZoneID: "east", Upstream: "u", Downstream: "d",
		BoundaryValves: []string{"east-bv-a", "east-bv-b"}, ReliefValve: "east-rv",
	}})
	registry := valve.NewRegistry(dir, ttl)
	relief := valve.NewReliefService(registry, reliefDelay)
	boundaries := valve.NewBoundaryService(registry)
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(dir, "events.jsonl")))
	service := NewService(dir, topology, state, registry, relief, boundaries, recorder, ttl)
	return service, registry, relief, boundaries, state
}

func TestIsolateAndRestoreFullLifecycle(t *testing.T) {
	service, _, relief, boundaries, state := testHarness(t, t.TempDir(), time.Minute, 0)
	ctx := context.Background()

	session, err := service.Isolate(ctx, "east-branch", "operator")
	if err != nil {
		t.Fatalf("isolate: %v", err)
	}
	if session.Status != StatusCompleted {
		t.Fatalf("expected completed, got %s (%s)", session.Status, session.Error)
	}
	if !relief.SafeOpen("east-rv") {
		t.Fatal("relief must be proven open while isolated")
	}
	for _, id := range []string{"east-bv-a", "east-bv-b"} {
		if !boundaries.SafeClosed(id) {
			t.Fatalf("boundary %s must be proven closed", id)
		}
	}
	if session.Relief.SessionID != session.ID {
		t.Fatal("relief lifecycle must correlate to session")
	}
	for _, lc := range session.Boundaries {
		if lc.SessionID != session.ID {
			t.Fatalf("boundary lifecycle %s not correlated to session %s", lc.ID, session.ID)
		}
	}
	zone, _ := state.Get("east")
	if zone.Phase != network.PhaseIsolated {
		t.Fatalf("zone phase = %s, want isolated", zone.Phase)
	}

	restored, err := service.Restore(ctx, session.ID, "operator")
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.Status != StatusRestored {
		t.Fatalf("expected restored, got %s", restored.Status)
	}
	if relief.SafeOpen("east-rv") {
		t.Fatal("after verified restore relief must not report open")
	}
	for _, id := range []string{"east-bv-a", "east-bv-b"} {
		if boundaries.SafeClosed(id) {
			t.Fatalf("boundary %s must no longer report closed after verified re-open", id)
		}
	}
	if restored.Relief.Phase != valve.PhaseReleased {
		t.Fatalf("relief should be released, got %s", restored.Relief.Phase)
	}
	for _, lc := range restored.Boundaries {
		if lc.Phase != valve.PhaseReleased {
			t.Fatalf("boundary %s should be released, got %s", lc.ValveID, lc.Phase)
		}
	}
}

func TestIsolateFailureRecordsFailedLifecycles(t *testing.T) {
	service, registry, _, boundaries, state := testHarness(t, t.TempDir(), time.Minute, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	session, err := service.Isolate(ctx, "east-branch", "operator")
	if err == nil {
		t.Fatal("expected isolation failure")
	}
	if session.Status != StatusFailed {
		t.Fatalf("expected failed session, got %s", session.Status)
	}
	history := registry.History(session.ID, 0)
	if len(history) != 1 || history[0].Phase != valve.PhaseFailed || !history[0].ConfirmedAt.IsZero() {
		t.Fatalf("expected a single unconfirmed failed relief lifecycle, got %+v", history)
	}
	if boundaries.SafeClosed("east-bv-a") {
		t.Fatal("failed isolation must not claim closed boundaries")
	}
	zone, _ := state.Get("east")
	if zone.Phase == network.PhaseIsolated {
		t.Fatal("zone must not be marked isolated on failure")
	}
}

func TestRestartDoesNotTreatUnconfirmedPositionsAsSafe(t *testing.T) {
	dir := t.TempDir()
	service1, _, relief1, boundaries1, _ := testHarness(t, dir, time.Minute, 0)
	session, err := service1.Isolate(context.Background(), "east-branch", "operator")
	if err != nil {
		t.Fatalf("isolate: %v", err)
	}
	if !relief1.SafeOpen("east-rv") || !boundaries1.SafeClosed("east-bv-a") {
		t.Fatal("pre-restart should be trusted")
	}

	// New process: rebuild everything from the same data directory.
	service2, _, relief2, boundaries2, _ := testHarness(t, dir, time.Minute, 0)
	if err := service2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	recovered, ok := service2.Get(session.ID)
	if !ok {
		t.Fatal("session must survive restart")
	}
	if recovered.Status != StatusIndeterminate {
		t.Fatalf("completed session must become indeterminate after restart, got %s", recovered.Status)
	}
	if relief2.SafeOpen("east-rv") {
		t.Fatal("relief position after restart must not be presumed open")
	}
	if boundaries2.SafeClosed("east-bv-a") {
		t.Fatal("boundary position after restart must not be presumed safely closed")
	}

	// Indeterminate sessions cannot be auto-restored.
	if _, err := service2.Restore(context.Background(), session.ID, "operator"); err == nil {
		t.Fatal("restore of indeterminate session must require field inspection")
	}
	// And they block starting a new isolation on the same section.
	if _, err := service2.Isolate(context.Background(), "east-branch", "operator"); err == nil {
		t.Fatal("indeterminate session must block re-isolation")
	}
}

func TestInterruptedSessionFailsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	service1, registry, _, _, _ := testHarness(t, dir, time.Minute, 0)
	// Persist a session that never finished closing boundaries.
	interrupted := Session{
		ID: "sess-stuck", SectionID: "east-branch", ZoneID: "east",
		Status: StatusClosingBoundary, StartedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := service1.persist(interrupted); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Request("east-bv-a", "boundary", interrupted.ID, valve.DirectionClosing, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	service2, _, _, _, _ := testHarness(t, dir, time.Minute, 0)
	if err := service2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	got, ok := service2.Get(interrupted.ID)
	if !ok || got.Status != StatusFailed {
		t.Fatalf("interrupted session must be failed, got %+v", got)
	}
}

func TestHoldTimeoutInvalidatesCompletedSession(t *testing.T) {
	service, registry, relief, boundaries, _ := testHarness(t, t.TempDir(), time.Minute, 0)
	session, err := service.Isolate(context.Background(), "east-branch", "operator")
	if err != nil {
		t.Fatalf("isolate: %v", err)
	}

	// Simulate the field losing the relief valve: its lifecycle fails. The
	// next maintenance sweep must invalidate the session instead of claiming
	// the (now unconfirmed) boundary positions are still safely closed.
	if _, err := registry.Fail("east-rv", session.ID, time.Now().UTC(), "simulated field fault"); err != nil {
		t.Fatalf("fail relief: %v", err)
	}
	service.sweepAt(time.Now().UTC())

	got, _ := service.Get(session.ID)
	if got.Status != StatusIndeterminate {
		t.Fatalf("session should be invalidated after relief proof failed, got %s", got.Status)
	}
	if relief.SafeOpen("east-rv") || boundaries.SafeClosed("east-bv-a") {
		t.Fatal("invalidated session positions must not be trusted safe")
	}
}

func TestSweepFailsStaleLifecycleAndInvalidatesSession(t *testing.T) {
	ttl := time.Minute
	service, registry, _, _, _ := testHarness(t, t.TempDir(), ttl, 0)
	session, err := service.Isolate(context.Background(), "east-branch", "operator")
	if err != nil {
		t.Fatalf("isolate: %v", err)
	}
	// No hold renewals arrive; sweep well past the lease (+ grace).
	service.sweepAt(time.Now().UTC().Add(3 * ttl))
	failed := registry.History(session.ID, 0)
	if len(failed) != 3 { // relief + two boundaries
		t.Fatalf("expected 3 failed lifecycles, got %d", len(failed))
	}
	for _, lc := range failed {
		if lc.Phase != valve.PhaseFailed {
			t.Fatalf("lifecycle %s should be failed, got %s", lc.ValveID, lc.Phase)
		}
	}
	got, _ := service.Get(session.ID)
	if got.Status != StatusIndeterminate {
		t.Fatalf("session must be indeterminate after stale sweep, got %s", got.Status)
	}
}

func TestHoldRenewalKeepsIsolationTrusted(t *testing.T) {
	service, _, relief, boundaries, _ := testHarness(t, t.TempDir(), time.Minute, 0)
	session, err := service.Isolate(context.Background(), "east-branch", "operator")
	if err != nil {
		t.Fatalf("isolate: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := relief.Hold("east-rv", session.ID); err != nil {
			t.Fatalf("renew relief hold: %v", err)
		}
		if _, err := boundaries.Hold("east-bv-a", session.ID); err != nil {
			t.Fatalf("renew boundary hold: %v", err)
		}
	}
	if !relief.SafeOpen("east-rv") || !boundaries.SafeClosed("east-bv-a") {
		t.Fatal("freshly renewed holds must stay trusted")
	}
	// A sweep within the lease must leave everything intact.
	service.sweepAt(time.Now().UTC())
	if !relief.SafeOpen("east-rv") || !boundaries.SafeClosed("east-bv-a") {
		t.Fatal("sweep within lease must not invalidate proven positions")
	}
}
