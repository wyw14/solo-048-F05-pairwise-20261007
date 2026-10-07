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

func newTestService(t *testing.T, reliefDelay, reliefTimeout time.Duration) *Service {
	t.Helper()
	dir := t.TempDir()
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(dir, "events.jsonl")))
	topology := network.NewTopology([]network.Section{
		{ID: "sec-1", ZoneID: "zone-1", Upstream: "up", Downstream: "down", BoundaryValves: []string{"bv-a", "bv-b"}, ReliefValve: "rv-1"},
	})
	state := network.NewState([]network.Zone{{ID: "zone-1", Phase: network.PhaseActive}})
	journal := valve.NewLifecycleJournal(filepath.Join(dir, "valve_lifecycle.json"))
	relief, err := valve.NewReliefService(journal, reliefDelay, reliefTimeout)
	if err != nil {
		t.Fatal(err)
	}
	boundaries, err := valve.NewBoundaryService(journal, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return NewService(topology, state, relief, boundaries, journal, recorder)
}

func TestIsolateRecordsCorrelatedLifecycle(t *testing.T) {
	service := newTestService(t, 0, time.Second)
	session, err := service.Isolate(context.Background(), "sec-1", "tester")
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != StatusCompleted {
		t.Fatalf("session status = %s, want completed", session.Status)
	}
	if len(session.Operations) != 3 {
		t.Fatalf("expected 3 recorded operations, got %d", len(session.Operations))
	}
	proofs, err := service.Proofs(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	// 3 valves × (申请 + 到位 + 保持 + 关闭验证)
	if len(proofs) != 12 {
		t.Fatalf("expected 12 lifecycle proofs, got %d", len(proofs))
	}
	stages := map[valve.LifecycleStage]int{}
	for _, proof := range proofs {
		if proof.SessionID != session.ID {
			t.Fatalf("proof %s carries session %s", proof.ID, proof.SessionID)
		}
		stages[proof.Stage]++
	}
	for stage, want := range map[valve.LifecycleStage]int{
		valve.StageRequested: 3, valve.StageActuated: 3, valve.StageHolding: 3, valve.StageCloseVerified: 3,
	} {
		if stages[stage] != want {
			t.Fatalf("stage %s recorded %d times, want %d", stage, stages[stage], want)
		}
	}
	zones := service.state.List()
	if len(zones) != 1 || zones[0].Phase != network.PhaseIsolated {
		t.Fatalf("zone phase = %+v, want isolated", zones)
	}
}

func TestIsolateFailureFailsSessionOperations(t *testing.T) {
	// Relief confirmation times out: the session must fail and no valve
	// operation may be left looking confirmed.
	service := newTestService(t, 50*time.Millisecond, time.Millisecond)
	session, err := service.Isolate(context.Background(), "sec-1", "tester")
	if err == nil {
		t.Fatal("expected isolation to fail on relief timeout")
	}
	if session.Status != StatusFailed {
		t.Fatalf("session status = %s, want failed", session.Status)
	}
	proofs, err := service.Proofs(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(proofs) == 0 {
		t.Fatal("failed session must still record its lifecycle proofs")
	}
	lastByOp := map[string]valve.LifecycleProof{}
	for _, proof := range proofs {
		if proof.SessionID != session.ID {
			t.Fatalf("proof %s carries session %s", proof.ID, proof.SessionID)
		}
		lastByOp[proof.OperationID] = proof
	}
	for operationID, proof := range lastByOp {
		if proof.Stage != valve.StageFailed && proof.Stage != valve.StageCloseVerified {
			t.Fatalf("operation %s left in non-terminal stage %s", operationID, proof.Stage)
		}
		if proof.Stage == valve.StageFailed && proof.Position != valve.PositionUnknown {
			t.Fatalf("failed operation %s carries position %s, want unknown", operationID, proof.Position)
		}
	}
	if service.relief.IsOpen("rv-1") {
		t.Fatal("relief valve with unconfirmed position must not report open")
	}
}
