package valve

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/store"
)

// LifecycleStage is one recorded step of a relief/boundary valve operation:
// 申请 (requested) → 打开/到位 (actuated) → 保持 (holding) → 关闭验证 (close_verified),
// with 失败 (failed) reachable from every non-terminal stage.
type LifecycleStage string

const (
	StageRequested     LifecycleStage = "requested"
	StageActuated      LifecycleStage = "actuated"
	StageHolding       LifecycleStage = "holding"
	StageCloseVerified LifecycleStage = "close_verified"
	StageFailed        LifecycleStage = "failed"
)

// PositionConfidence is the confirmed valve position. PositionUnknown is the
// fail-safe default: an unconfirmed position is never treated as safely closed.
type PositionConfidence string

const (
	PositionUnknown PositionConfidence = "unknown"
	PositionOpen    PositionConfidence = "open"
	PositionClosed  PositionConfidence = "closed"
)

// Valve kinds recorded in the lifecycle journal.
const (
	KindRelief   = "relief"
	KindBoundary = "boundary"
)

// LifecycleProof records one stage transition of a valve operation. Every
// proof carries the isolation session ID and the operation ID, so all proofs
// of one isolation session can be correlated.
type LifecycleProof struct {
	ID          string             `json:"id"`
	SessionID   string             `json:"session_id"`
	OperationID string             `json:"operation_id"`
	ValveID     string             `json:"valve_id"`
	Kind        string             `json:"kind"`
	Stage       LifecycleStage     `json:"stage"`
	Position    PositionConfidence `json:"position"`
	Detail      string             `json:"detail,omitempty"`
	At          time.Time          `json:"at"`
}

// OperationState is the current lifecycle state of one valve operation.
type OperationState struct {
	OperationID string             `json:"operation_id"`
	SessionID   string             `json:"session_id"`
	ValveID     string             `json:"valve_id"`
	Stage       LifecycleStage     `json:"stage"`
	Position    PositionConfidence `json:"position"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

func terminalStage(stage LifecycleStage) bool {
	return stage == StageCloseVerified || stage == StageFailed
}

var lifecycleTransitions = map[LifecycleStage][]LifecycleStage{
	StageRequested: {StageActuated, StageFailed},
	StageActuated:  {StageHolding, StageCloseVerified, StageFailed},
	StageHolding:   {StageHolding, StageCloseVerified, StageFailed},
}

// LifecycleJournal persists every lifecycle proof so that valve positions
// survive restarts and can be audited per isolation session.
type LifecycleJournal struct {
	file *store.File[[]LifecycleProof]
}

func NewLifecycleJournal(path string) *LifecycleJournal {
	return &LifecycleJournal{file: store.NewFile[[]LifecycleProof](path)}
}

func (j *LifecycleJournal) Append(proof LifecycleProof) (LifecycleProof, error) {
	if proof.SessionID == "" || proof.OperationID == "" || proof.ValveID == "" {
		return LifecycleProof{}, fmt.Errorf("lifecycle proof identity is incomplete")
	}
	if proof.ID == "" {
		proof.ID = uuid.NewString()
	}
	if proof.At.IsZero() {
		proof.At = time.Now().UTC()
	}
	if _, err := j.file.Update(func(all *[]LifecycleProof) error {
		*all = append(*all, proof)
		return nil
	}); err != nil {
		return LifecycleProof{}, fmt.Errorf("persist lifecycle proof: %w", err)
	}
	return proof, nil
}

func (j *LifecycleJournal) List() ([]LifecycleProof, error) {
	proofs, err := j.file.Load()
	if err != nil {
		return nil, fmt.Errorf("load lifecycle journal: %w", err)
	}
	return proofs, nil
}

// ForSession returns every proof recorded under one isolation session, oldest first.
func (j *LifecycleJournal) ForSession(sessionID string) ([]LifecycleProof, error) {
	proofs, err := j.List()
	if err != nil {
		return nil, err
	}
	result := make([]LifecycleProof, 0)
	for _, proof := range proofs {
		if proof.SessionID == sessionID {
			result = append(result, proof)
		}
	}
	return result, nil
}

// lifecycle drives one valve family's operations through their recorded stages.
type lifecycle struct {
	kind    string
	journal *LifecycleJournal
	mu      sync.Mutex
	ops     map[string]*OperationState
	byValve map[string]string
}

func newLifecycle(kind string, journal *LifecycleJournal) *lifecycle {
	return &lifecycle{kind: kind, journal: journal, ops: make(map[string]*OperationState), byValve: make(map[string]string)}
}

// Request records 申请: a new operation for a valve. A valve may only have one
// non-terminal operation at a time.
func (l *lifecycle) Request(sessionID, valveID string) (OperationState, error) {
	if sessionID == "" || valveID == "" {
		return OperationState{}, fmt.Errorf("%s operation requires session and valve", l.kind)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if opID := l.byValve[valveID]; opID != "" {
		if op := l.ops[opID]; op != nil && !terminalStage(op.Stage) {
			return OperationState{}, fmt.Errorf("%s valve %s already has active operation %s", l.kind, valveID, op.OperationID)
		}
	}
	op := &OperationState{
		OperationID: uuid.NewString(), SessionID: sessionID, ValveID: valveID,
		Stage: StageRequested, Position: PositionUnknown, UpdatedAt: time.Now().UTC(),
	}
	if _, err := l.appendLocked(op, "operation requested"); err != nil {
		return OperationState{}, err
	}
	l.ops[op.OperationID] = op
	l.byValve[valveID] = op.OperationID
	return *op, nil
}

// Actuate records 打开/到位: the valve confirmed the commanded position.
func (l *lifecycle) Actuate(operationID string, position PositionConfidence, detail string) (LifecycleProof, error) {
	return l.move(operationID, StageActuated, position, detail)
}

// Hold records 保持: the valve is reaffirmed in its current confirmed position.
func (l *lifecycle) Hold(operationID, detail string) (LifecycleProof, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	op, err := l.getLocked(operationID)
	if err != nil {
		return LifecycleProof{}, err
	}
	return l.moveLocked(op, StageHolding, op.Position, detail)
}

// VerifyClosed records 关闭验证: the closed position was independently verified.
func (l *lifecycle) VerifyClosed(operationID, detail string) (LifecycleProof, error) {
	return l.move(operationID, StageCloseVerified, PositionClosed, detail)
}

// Fail records 失败: the operation failed and the position is unconfirmed.
func (l *lifecycle) Fail(operationID, detail string) (LifecycleProof, error) {
	return l.move(operationID, StageFailed, PositionUnknown, detail)
}

// FailSession fails every non-terminal operation of one isolation session.
func (l *lifecycle) FailSession(sessionID, detail string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var first error
	for _, op := range l.ops {
		if op.SessionID != sessionID || terminalStage(op.Stage) {
			continue
		}
		if _, err := l.moveLocked(op, StageFailed, PositionUnknown, detail); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (l *lifecycle) move(operationID string, stage LifecycleStage, position PositionConfidence, detail string) (LifecycleProof, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	op, err := l.getLocked(operationID)
	if err != nil {
		return LifecycleProof{}, err
	}
	return l.moveLocked(op, stage, position, detail)
}

func (l *lifecycle) moveLocked(op *OperationState, stage LifecycleStage, position PositionConfidence, detail string) (LifecycleProof, error) {
	allowed := false
	for _, next := range lifecycleTransitions[op.Stage] {
		if next == stage {
			allowed = true
			break
		}
	}
	if !allowed {
		return LifecycleProof{}, fmt.Errorf("%s operation %s cannot move from %s to %s", l.kind, op.OperationID, op.Stage, stage)
	}
	op.Stage = stage
	op.Position = position
	return l.appendLocked(op, detail)
}

func (l *lifecycle) appendLocked(op *OperationState, detail string) (LifecycleProof, error) {
	proof, err := l.journal.Append(LifecycleProof{
		SessionID: op.SessionID, OperationID: op.OperationID, ValveID: op.ValveID,
		Kind: l.kind, Stage: op.Stage, Position: op.Position, Detail: detail,
	})
	if err != nil {
		return LifecycleProof{}, err
	}
	op.UpdatedAt = proof.At
	return proof, nil
}

func (l *lifecycle) getLocked(operationID string) (*OperationState, error) {
	op, ok := l.ops[operationID]
	if !ok {
		return nil, fmt.Errorf("%s operation %s not found", l.kind, operationID)
	}
	return op, nil
}

func (l *lifecycle) State(operationID string) (OperationState, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	op, ok := l.ops[operationID]
	if !ok {
		return OperationState{}, false
	}
	return *op, true
}

func (l *lifecycle) ValveState(valveID string) (OperationState, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	op, ok := l.ops[l.byValve[valveID]]
	if !ok {
		return OperationState{}, false
	}
	return *op, true
}

// ConfirmedPosition returns the last confirmed position of a valve. Anything
// that was never confirmed — requested only, failed, or lost to a restart —
// reports PositionUnknown and must never be treated as safely closed.
func (l *lifecycle) ConfirmedPosition(valveID string) PositionConfidence {
	l.mu.Lock()
	defer l.mu.Unlock()
	op, ok := l.ops[l.byValve[valveID]]
	if !ok {
		return PositionUnknown
	}
	switch op.Stage {
	case StageActuated, StageHolding, StageCloseVerified:
		return op.Position
	default:
		return PositionUnknown
	}
}

// Recover rebuilds operation state from the journal after a restart. Any
// operation that did not reach a terminal stage is marked failed with an
// unknown position: an unconfirmed valve is never treated as safely closed.
func (l *lifecycle) Recover() error {
	proofs, err := l.journal.List()
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, proof := range proofs {
		if proof.Kind != l.kind {
			continue
		}
		op := l.ops[proof.OperationID]
		if op == nil {
			op = &OperationState{OperationID: proof.OperationID, SessionID: proof.SessionID, ValveID: proof.ValveID}
			l.ops[proof.OperationID] = op
		}
		op.Stage = proof.Stage
		op.Position = proof.Position
		op.UpdatedAt = proof.At
		l.byValve[proof.ValveID] = proof.OperationID
	}
	for _, op := range l.ops {
		if terminalStage(op.Stage) {
			continue
		}
		op.Stage = StageFailed
		op.Position = PositionUnknown
		if _, err := l.appendLocked(op, "service restarted before confirmation; position unconfirmed"); err != nil {
			return err
		}
	}
	return nil
}
