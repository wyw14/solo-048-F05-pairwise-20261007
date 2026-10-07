package valve

import (
	"context"
	"testing"
	"time"
)

func newTestRegistry(t *testing.T, ttl time.Duration) *Registry {
	t.Helper()
	return NewRegistry(t.TempDir(), ttl)
}

func TestReliefLifecycleRequestEstablishHoldRelease(t *testing.T) {
	registry := newTestRegistry(t, time.Second)
	relief := NewReliefService(registry, 0)
	ctx := context.Background()

	open, err := relief.Open(ctx, "rv-1", "sess-1")
	if err != nil {
		t.Fatalf("open relief: %v", err)
	}
	if open.SessionID != "sess-1" || open.Phase != PhaseEstablished || open.ConfirmedAt.IsZero() {
		t.Fatalf("unexpected established lifecycle: %+v", open)
	}
	if !relief.SafeOpen("rv-1") {
		t.Fatal("relief must be trusted open after establishment")
	}

	held, err := relief.Hold("rv-1", "sess-1")
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	if held.Phase != PhaseHeld {
		t.Fatalf("expected held, got %s", held.Phase)
	}

	closed, err := relief.VerifyClose(ctx, "rv-1", "sess-1")
	if err != nil {
		t.Fatalf("verify close: %v", err)
	}
	if closed.Phase != PhaseReleased || closed.ReleasedAt.IsZero() {
		t.Fatalf("expected verified release, got %+v", closed)
	}
	if relief.SafeOpen("rv-1") {
		t.Fatal("released relief must not report safe open")
	}

	history := registry.History("sess-1", 0)
	if len(history) != 1 {
		t.Fatalf("expected 1 correlated lifecycle, got %d", len(history))
	}
	phases := map[Phase]bool{}
	for _, tr := range history[0].Transitions {
		phases[tr.Phase] = true
	}
	for _, want := range []Phase{PhaseRequested, PhaseEstablished, PhaseHeld, PhaseReleased} {
		if !phases[want] {
			t.Fatalf("transition %s missing from %v", want, phases)
		}
	}
}

func TestReliefOpenTimeoutFailsUnconfirmed(t *testing.T) {
	registry := newTestRegistry(t, time.Second)
	relief := NewReliefService(registry, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := relief.Open(ctx, "rv-1", "sess-1"); err == nil {
		t.Fatal("expected timeout error")
	}
	if relief.SafeOpen("rv-1") {
		t.Fatal("timed-out relief must never be treated as safely open")
	}
	history := registry.History("sess-1", 0)
	if len(history) != 1 || history[0].Phase != PhaseFailed || history[0].ConfirmedAt.IsZero() != true {
		t.Fatalf("expected unconfirmed failed lifecycle, got %+v", history)
	}
}

func TestDriverUnavailableFails(t *testing.T) {
	registry := newTestRegistry(t, time.Second)
	relief := NewReliefService(registry, -1)
	if _, err := relief.Open(context.Background(), "rv-1", "sess-1"); err == nil {
		t.Fatal("expected driver failure")
	}
	if relief.SafeOpen("rv-1") {
		t.Fatal("failed driver must not yield trusted open position")
	}
}

func TestBoundaryRequiresFreshReliefProof(t *testing.T) {
	registry := newTestRegistry(t, time.Minute)
	relief := NewReliefService(registry, 0)
	boundaries := NewBoundaryService(registry)

	if _, err := boundaries.Close(context.Background(), "bv-a", "sess-1", "rv-1"); err == nil {
		t.Fatal("boundary closure without open relief must be rejected")
	}

	if _, err := relief.Open(context.Background(), "rv-1", "sess-A"); err != nil {
		t.Fatalf("open relief: %v", err)
	}
	// A different session presents itself: the relief proof exists but is not
	// correlated to this session, so closure must be blocked.
	if _, err := boundaries.Close(context.Background(), "bv-a", "sess-B", "rv-1"); err == nil {
		t.Fatal("cross-session boundary closure must be rejected")
	}
	if boundaries.SafeClosed("bv-a") {
		t.Fatal("rejected boundary request must not be treated as closed")
	}

	closed, err := boundaries.Close(context.Background(), "bv-a", "sess-A", "rv-1")
	if err != nil {
		t.Fatalf("boundary close with correlated fresh proof: %v", err)
	}
	if closed.SessionID != "sess-A" || closed.Phase != PhaseEstablished {
		t.Fatalf("unexpected boundary lifecycle: %+v", closed)
	}
	if !boundaries.SafeClosed("bv-a") {
		t.Fatal("boundary must report safe closed after field establishment")
	}
}

func TestBoundaryCloseTimeoutFails(t *testing.T) {
	registry := newTestRegistry(t, time.Minute)
	relief := NewReliefService(registry, 0)
	boundaries := NewBoundaryService(registry)
	if _, err := relief.Open(context.Background(), "rv-1", "sess-1"); err != nil {
		t.Fatalf("open relief: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := boundaries.Close(ctx, "bv-a", "sess-1", "rv-1"); err == nil {
		t.Fatal("expected boundary timeout error")
	}
	if boundaries.SafeClosed("bv-a") {
		t.Fatal("timed-out boundary must never be treated as safely closed")
	}
	history := registry.History("sess-1", 0)
	var found bool
	for _, lc := range history {
		if lc.ValveID == "bv-a" && lc.Phase == PhaseFailed {
			found = true
		}
	}
	if !found {
		t.Fatal("expected failed boundary lifecycle correlated to session")
	}
}

func TestStaleHoldIsNotTrustedAndSweepFails(t *testing.T) {
	ttl := 30 * time.Millisecond
	registry := newTestRegistry(t, ttl)
	relief := NewReliefService(registry, 0)
	if _, err := relief.Open(context.Background(), "rv-1", "sess-1"); err != nil {
		t.Fatalf("open: %v", err)
	}
	time.Sleep(2 * ttl)
	if relief.SafeOpen("rv-1") {
		t.Fatal("stale confirmation must not be trusted as open")
	}
	failed, err := registry.Sweep(time.Now().UTC())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(failed) != 1 || failed[0].Phase != PhaseFailed {
		t.Fatalf("expected 1 failed lifecycle, got %+v", failed)
	}
	if relief.SafeOpen("rv-1") {
		t.Fatal("swept relief must not be trusted open")
	}
	if _, err := relief.Hold("rv-1", "sess-1"); err == nil {
		t.Fatal("hold on failed lifecycle must be rejected")
	}
}

func TestCrossSessionTransitionsRejected(t *testing.T) {
	registry := newTestRegistry(t, time.Minute)
	relief := NewReliefService(registry, 0)
	if _, err := relief.Open(context.Background(), "rv-1", "sess-A"); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := relief.Hold("rv-1", "sess-X"); err == nil {
		t.Fatal("hold from another session must be rejected")
	}
	if _, err := relief.VerifyClose(context.Background(), "rv-1", "sess-X"); err == nil {
		t.Fatal("close verification from another session must be rejected")
	}
	if !relief.SafeOpen("rv-1") {
		t.Fatal("rejected foreign transitions must not disturb the owning session")
	}
}

func TestRestartRecoveryFailsUnconfirmedPositions(t *testing.T) {
	dir := t.TempDir()
	ttl := time.Minute

	registry1 := NewRegistry(dir, ttl)
	relief1 := NewReliefService(registry1, 0)
	boundaries1 := NewBoundaryService(registry1)
	if _, err := relief1.Open(context.Background(), "rv-1", "sess-1"); err != nil {
		t.Fatalf("open relief: %v", err)
	}
	if _, err := boundaries1.Close(context.Background(), "bv-a", "sess-1", "rv-1"); err != nil {
		t.Fatalf("close boundary: %v", err)
	}
	if !relief1.SafeOpen("rv-1") || !boundaries1.SafeClosed("bv-a") {
		t.Fatal("pre-restart positions should be trusted")
	}

	// Simulate a fresh process rebuilding services from the same data dir.
	registry2 := NewRegistry(dir, ttl)
	recovered, err := registry2.Recover(time.Now().UTC())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(recovered) != 2 {
		t.Fatalf("expected 2 recovered lifecycles, got %d", len(recovered))
	}
	for _, lc := range recovered {
		if lc.Phase != PhaseFailed {
			t.Fatalf("recovered lifecycle must be failed, got %s: %+v", lc.Phase, lc)
		}
	}
	relief2 := NewReliefService(registry2, 0)
	boundaries2 := NewBoundaryService(registry2)
	if relief2.SafeOpen("rv-1") {
		t.Fatal("after restart relief must not be presumed open")
	}
	if boundaries2.SafeClosed("bv-a") {
		t.Fatal("after restart boundary must not be presumed safely closed")
	}
	view := boundaries2.View("bv-a")
	if view.Trusted {
		t.Fatalf("post-restart view must be untrusted: %+v", view)
	}

	// Recovery twice must be idempotent (no active records left).
	again, err := registry2.Recover(time.Now().UTC())
	if err != nil || len(again) != 0 {
		t.Fatalf("second recovery should be no-op, got %d / %v", len(again), err)
	}
}

func TestVerifiedReleaseSurvivesRestartAsSafe(t *testing.T) {
	dir := t.TempDir()
	registry1 := NewRegistry(dir, time.Minute)
	relief1 := NewReliefService(registry1, 0)
	if _, err := relief1.Open(context.Background(), "rv-1", "sess-1"); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := relief1.VerifyClose(context.Background(), "rv-1", "sess-1"); err != nil {
		t.Fatalf("verify close: %v", err)
	}
	registry2 := NewRegistry(dir, time.Minute)
	recovered, err := registry2.Recover(time.Now().UTC())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("released lifecycle is archived, not active; expected no recovery entries, got %+v", recovered)
	}
	history := registry2.History("sess-1", 0)
	if len(history) != 1 || history[0].Phase != PhaseReleased {
		t.Fatalf("verified release must be preserved across restart, got %+v", history)
	}
}

func TestPositionViews(t *testing.T) {
	registry := newTestRegistry(t, time.Minute)
	relief := NewReliefService(registry, 0)
	boundaries := NewBoundaryService(registry)

	unknown := relief.View("never-touched")
	if unknown.Phase != PhaseUnknown || unknown.Trusted {
		t.Fatalf("untracked valve must be unknown/untrusted: %+v", unknown)
	}

	if _, err := relief.Open(context.Background(), "rv-1", "sess-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := boundaries.Close(context.Background(), "bv-a", "sess-1", "rv-1"); err != nil {
		t.Fatal(err)
	}
	openView := relief.View("rv-1")
	if !openView.Trusted || !openView.Open || openView.SessionID != "sess-1" {
		t.Fatalf("relief view wrong: %+v", openView)
	}
	closedView := boundaries.View("bv-a")
	if !closedView.Trusted || closedView.Open || closedView.Phase != PhaseEstablished {
		t.Fatalf("boundary view wrong: %+v", closedView)
	}

	if _, err := boundaries.VerifyOpen(context.Background(), "bv-a", "sess-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := relief.VerifyClose(context.Background(), "rv-1", "sess-1"); err != nil {
		t.Fatal(err)
	}
	releasedRelief := relief.View("rv-1")
	if !releasedRelief.Trusted || releasedRelief.Open || releasedRelief.Phase != PhaseReleased {
		t.Fatalf("released relief should be trusted resting(closed): %+v", releasedRelief)
	}
	releasedBoundary := boundaries.View("bv-a")
	if !releasedBoundary.Trusted || !releasedBoundary.Open || releasedBoundary.Phase != PhaseReleased {
		t.Fatalf("released boundary should be trusted resting(open): %+v", releasedBoundary)
	}
}

func TestStaleReleaseNotTrusted(t *testing.T) {
	dir := t.TempDir()
	registry := NewRegistry(dir, 10*time.Millisecond)
	relief := NewReliefService(registry, 0)
	if _, err := relief.Open(context.Background(), "rv-1", "sess-1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	view := relief.View("rv-1")
	if view.Trusted || view.Fresh {
		t.Fatalf("stale view must be untrusted: %+v", view)
	}
	// Renewal after lapse must fail rather than resurrecting stale evidence.
	if _, err := relief.Hold("rv-1", "sess-1"); err == nil {
		t.Fatal("stale hold must be rejected")
	}
	if relief.SafeOpen("rv-1") {
		t.Fatal("stale relief must not report safe open")
	}
}
