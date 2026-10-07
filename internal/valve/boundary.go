package valve

import (
	"context"
	"fmt"
	"time"
)

// BoundaryService drives boundary valves through their recorded lifecycle
// (申请 → 关闭到位 → 保持 → 关闭验证 / 失败) instead of issuing instantaneous proofs.
type BoundaryService struct {
	lifecycle *lifecycle
	delay     time.Duration
	timeout   time.Duration
}

func NewBoundaryService(journal *LifecycleJournal, delay, confirmTimeout time.Duration) (*BoundaryService, error) {
	service := &BoundaryService{lifecycle: newLifecycle(KindBoundary, journal), delay: delay, timeout: confirmTimeout}
	if err := service.lifecycle.Recover(); err != nil {
		return nil, fmt.Errorf("recover boundary lifecycle: %w", err)
	}
	return service, nil
}

// Request records 申请 for a boundary valve operation.
func (s *BoundaryService) Request(sessionID, valveID string) (OperationState, error) {
	return s.lifecycle.Request(sessionID, valveID)
}

// Close records 到位 once the valve confirms the closed position. It requires a
// current relief proof from the same isolation session. On timeout the
// operation is failed and the position stays unconfirmed — never assumed closed.
func (s *BoundaryService) Close(ctx context.Context, operationID string, relief LifecycleProof) (LifecycleProof, error) {
	op, ok := s.lifecycle.State(operationID)
	if !ok {
		return LifecycleProof{}, fmt.Errorf("boundary operation %s not found", operationID)
	}
	if err := ctx.Err(); err != nil {
		_, _ = s.lifecycle.Fail(operationID, "context closed before actuation")
		return LifecycleProof{}, err
	}
	if !currentReliefProof(relief, op.SessionID) {
		return LifecycleProof{}, fmt.Errorf("boundary closure requires current relief proof for session %s", op.SessionID)
	}
	if err := s.await(ctx, op.ValveID, "close"); err != nil {
		_, _ = s.lifecycle.Fail(operationID, "close confirmation timeout: position unconfirmed")
		return LifecycleProof{}, err
	}
	return s.lifecycle.Actuate(operationID, PositionClosed, "boundary valve closed confirmed")
}

func currentReliefProof(proof LifecycleProof, sessionID string) bool {
	if proof.Kind != KindRelief || proof.SessionID != sessionID || proof.Position != PositionOpen {
		return false
	}
	return proof.Stage == StageActuated || proof.Stage == StageHolding
}

// Hold records 保持: the boundary valve is reaffirmed closed.
func (s *BoundaryService) Hold(operationID string) (LifecycleProof, error) {
	return s.lifecycle.Hold(operationID, "boundary valve holding closed")
}

// VerifyClosed records 关闭验证 once the closed position is independently
// confirmed. On timeout the position stays unconfirmed.
func (s *BoundaryService) VerifyClosed(ctx context.Context, operationID string) (LifecycleProof, error) {
	op, ok := s.lifecycle.State(operationID)
	if !ok {
		return LifecycleProof{}, fmt.Errorf("boundary operation %s not found", operationID)
	}
	if err := s.await(ctx, op.ValveID, "verify"); err != nil {
		_, _ = s.lifecycle.Fail(operationID, "close verification timeout: position unconfirmed")
		return LifecycleProof{}, err
	}
	return s.lifecycle.VerifyClosed(operationID, "boundary valve closed and verified")
}

// Fail records 失败 for one operation.
func (s *BoundaryService) Fail(operationID, detail string) (LifecycleProof, error) {
	return s.lifecycle.Fail(operationID, detail)
}

// FailSession fails every non-terminal boundary operation of an isolation session.
func (s *BoundaryService) FailSession(sessionID, detail string) error {
	return s.lifecycle.FailSession(sessionID, detail)
}

// IsClosed reports the confirmed position only. A valve whose position was
// never confirmed — or was lost to a timeout or restart — reports false.
func (s *BoundaryService) IsClosed(valveID string) bool {
	return s.lifecycle.ConfirmedPosition(valveID) == PositionClosed
}

// VerifiedClosed reports whether the closed position reached 关闭验证.
func (s *BoundaryService) VerifiedClosed(valveID string) bool {
	state, ok := s.lifecycle.ValveState(valveID)
	return ok && state.Stage == StageCloseVerified && state.Position == PositionClosed
}

// Position returns the confirmed position, PositionUnknown when unconfirmed.
func (s *BoundaryService) Position(valveID string) PositionConfidence {
	return s.lifecycle.ConfirmedPosition(valveID)
}

func (s *BoundaryService) await(ctx context.Context, valveID, action string) error {
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("boundary valve %s %s confirmation: %w", valveID, action, ctx.Err())
	case <-time.After(s.delay):
		return nil
	}
}
