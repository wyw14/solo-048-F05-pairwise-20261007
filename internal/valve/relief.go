package valve

import (
	"context"
	"fmt"
	"time"
)

// ReliefService drives relief valves through their recorded lifecycle
// (申请 → 打开 → 保持 → 关闭验证 / 失败) instead of issuing instantaneous proofs.
type ReliefService struct {
	lifecycle *lifecycle
	delay     time.Duration
	timeout   time.Duration
}

func NewReliefService(journal *LifecycleJournal, delay, confirmTimeout time.Duration) (*ReliefService, error) {
	service := &ReliefService{lifecycle: newLifecycle(KindRelief, journal), delay: delay, timeout: confirmTimeout}
	if err := service.lifecycle.Recover(); err != nil {
		return nil, fmt.Errorf("recover relief lifecycle: %w", err)
	}
	return service, nil
}

// Request records 申请 for a relief valve operation.
func (s *ReliefService) Request(sessionID, valveID string) (OperationState, error) {
	return s.lifecycle.Request(sessionID, valveID)
}

// Open records 打开 once the valve confirms the open position. On timeout the
// operation is failed and the position stays unconfirmed.
func (s *ReliefService) Open(ctx context.Context, operationID string) (LifecycleProof, error) {
	op, ok := s.lifecycle.State(operationID)
	if !ok {
		return LifecycleProof{}, fmt.Errorf("relief operation %s not found", operationID)
	}
	if s.delay < 0 {
		_, _ = s.lifecycle.Fail(operationID, "driver unavailable")
		return LifecycleProof{}, fmt.Errorf("relief valve %s driver unavailable", op.ValveID)
	}
	if err := s.await(ctx, op.ValveID, "open"); err != nil {
		_, _ = s.lifecycle.Fail(operationID, "open confirmation timeout: position unconfirmed")
		return LifecycleProof{}, err
	}
	return s.lifecycle.Actuate(operationID, PositionOpen, "relief valve open confirmed")
}

// Hold records 保持: the relief valve is reaffirmed open while boundaries move.
func (s *ReliefService) Hold(operationID string) (LifecycleProof, error) {
	return s.lifecycle.Hold(operationID, "relief valve holding open")
}

// VerifyClosed drives the relief valve closed and records 关闭验证 once the
// closed position is confirmed. On timeout the position stays unconfirmed.
func (s *ReliefService) VerifyClosed(ctx context.Context, operationID string) (LifecycleProof, error) {
	op, ok := s.lifecycle.State(operationID)
	if !ok {
		return LifecycleProof{}, fmt.Errorf("relief operation %s not found", operationID)
	}
	if err := s.await(ctx, op.ValveID, "close"); err != nil {
		_, _ = s.lifecycle.Fail(operationID, "close confirmation timeout: position unconfirmed")
		return LifecycleProof{}, err
	}
	return s.lifecycle.VerifyClosed(operationID, "relief valve closed and verified")
}

// Fail records 失败 for one operation.
func (s *ReliefService) Fail(operationID, detail string) (LifecycleProof, error) {
	return s.lifecycle.Fail(operationID, detail)
}

// FailSession fails every non-terminal relief operation of an isolation session.
func (s *ReliefService) FailSession(sessionID, detail string) error {
	return s.lifecycle.FailSession(sessionID, detail)
}

// IsOpen reports the confirmed position only; unconfirmed valves report closed=false.
func (s *ReliefService) IsOpen(valveID string) bool {
	return s.lifecycle.ConfirmedPosition(valveID) == PositionOpen
}

// Position returns the confirmed position, PositionUnknown when unconfirmed.
func (s *ReliefService) Position(valveID string) PositionConfidence {
	return s.lifecycle.ConfirmedPosition(valveID)
}

func (s *ReliefService) await(ctx context.Context, valveID, action string) error {
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("relief valve %s %s confirmation: %w", valveID, action, ctx.Err())
	case <-time.After(s.delay):
		return nil
	}
}
