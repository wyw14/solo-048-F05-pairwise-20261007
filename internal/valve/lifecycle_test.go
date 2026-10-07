package valve

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func testJournal(t *testing.T) *LifecycleJournal {
	t.Helper()
	return NewLifecycleJournal(filepath.Join(t.TempDir(), "valve_lifecycle.json"))
}

func reliefProofFor(sessionID string) LifecycleProof {
	return LifecycleProof{Kind: KindRelief, SessionID: sessionID, Stage: StageHolding, Position: PositionOpen}
}

func TestLifecycleProofsCorrelateBySession(t *testing.T) {
	journal := testJournal(t)
	relief, err := NewReliefService(journal, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	boundaries, err := NewBoundaryService(journal, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	reliefOp, err := relief.Request("session-1", "rv-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relief.Open(ctx, reliefOp.OperationID); err != nil {
		t.Fatal(err)
	}
	held, err := relief.Hold(reliefOp.OperationID)
	if err != nil {
		t.Fatal(err)
	}

	boundaryOp, err := boundaries.Request("session-1", "bv-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundaries.Close(ctx, boundaryOp.OperationID, held); err != nil {
		t.Fatal(err)
	}
	if _, err := boundaries.Hold(boundaryOp.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := boundaries.VerifyClosed(ctx, boundaryOp.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := relief.VerifyClosed(ctx, reliefOp.OperationID); err != nil {
		t.Fatal(err)
	}

	proofs, err := journal.ForSession("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(proofs) != 8 {
		t.Fatalf("expected 8 lifecycle proofs, got %d", len(proofs))
	}
	wantStages := []LifecycleStage{
		StageRequested, StageActuated, StageHolding,
		StageRequested, StageActuated, StageHolding, StageCloseVerified,
		StageCloseVerified,
	}
	for i, proof := range proofs {
		if proof.SessionID != "session-1" {
			t.Fatalf("proof %d carries session %s", i, proof.SessionID)
		}
		if proof.Stage != wantStages[i] {
			t.Fatalf("proof %d stage = %s, want %s", i, proof.Stage, wantStages[i])
		}
		if proof.OperationID != reliefOp.OperationID && proof.OperationID != boundaryOp.OperationID {
			t.Fatalf("proof %d carries unknown operation %s", i, proof.OperationID)
		}
	}
	if !boundaries.IsClosed("bv-1") || !boundaries.VerifiedClosed("bv-1") {
		t.Fatal("verified boundary valve must report closed")
	}
	if relief.IsOpen("rv-1") {
		t.Fatal("relief valve must report closed after close verification")
	}
}

func TestBoundaryCloseRequiresCurrentReliefProof(t *testing.T) {
	journal := testJournal(t)
	boundaries, err := NewBoundaryService(journal, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	op, err := boundaries.Request("session-1", "bv-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundaries.Close(context.Background(), op.OperationID, reliefProofFor("other-session")); err == nil {
		t.Fatal("close with a relief proof from another session must be rejected")
	}
	if boundaries.IsClosed("bv-1") {
		t.Fatal("rejected close must not report closed")
	}
}

func TestTimeoutLeavesPositionUnconfirmed(t *testing.T) {
	journal := testJournal(t)
	relief, err := NewReliefService(journal, 50*time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	boundaries, err := NewBoundaryService(journal, 50*time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	reliefOp, err := relief.Request("session-1", "rv-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relief.Open(ctx, reliefOp.OperationID); err == nil {
		t.Fatal("expected open confirmation timeout")
	}
	if relief.IsOpen("rv-1") {
		t.Fatal("relief valve with unconfirmed position must not report open")
	}
	if got := relief.Position("rv-1"); got != PositionUnknown {
		t.Fatalf("relief position = %s, want unknown", got)
	}

	boundaryOp, err := boundaries.Request("session-1", "bv-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundaries.Close(ctx, boundaryOp.OperationID, reliefProofFor("session-1")); err == nil {
		t.Fatal("expected close confirmation timeout")
	}
	if boundaries.IsClosed("bv-1") {
		t.Fatal("boundary valve with unconfirmed position must not report safely closed")
	}

	proofs, err := journal.ForSession("session-1")
	if err != nil {
		t.Fatal(err)
	}
	failures := 0
	for _, proof := range proofs {
		if proof.Stage == StageFailed {
			failures++
			if proof.Position != PositionUnknown {
				t.Fatalf("failed proof for %s carries position %s, want unknown", proof.ValveID, proof.Position)
			}
		}
	}
	if failures != 2 {
		t.Fatalf("expected 2 failure proofs, got %d", failures)
	}
}

func TestRestartFailsUnconfirmedOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valve_lifecycle.json")
	journal := NewLifecycleJournal(path)
	boundaries, err := NewBoundaryService(journal, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	op, err := boundaries.Request("session-1", "bv-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundaries.Close(context.Background(), op.OperationID, reliefProofFor("session-1")); err != nil {
		t.Fatal(err)
	}
	if !boundaries.IsClosed("bv-1") {
		t.Fatal("confirmed close must report closed before restart")
	}

	// Restart over the same journal: the close was actuated but never reached
	// 关闭验证, so it must not be treated as safely closed.
	restarted, err := NewBoundaryService(journal, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.IsClosed("bv-1") {
		t.Fatal("unverified closed position must not survive restart")
	}
	proofs, err := journal.ForSession("session-1")
	if err != nil {
		t.Fatal(err)
	}
	last := proofs[len(proofs)-1]
	if last.Stage != StageFailed || last.Position != PositionUnknown {
		t.Fatalf("restart proof = %s/%s, want failed/unknown", last.Stage, last.Position)
	}
}

func TestRestartKeepsVerifiedClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valve_lifecycle.json")
	journal := NewLifecycleJournal(path)
	boundaries, err := NewBoundaryService(journal, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	op, err := boundaries.Request("session-1", "bv-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := boundaries.Close(ctx, op.OperationID, reliefProofFor("session-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := boundaries.Hold(op.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := boundaries.VerifyClosed(ctx, op.OperationID); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewBoundaryService(journal, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.IsClosed("bv-1") || !restarted.VerifiedClosed("bv-1") {
		t.Fatal("verified closed position must survive restart")
	}
}
