package isolation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/network"
	"heatpulse/internal/valve"
)

type Status string

const (
	StatusOpeningRelief   Status = "opening_relief"
	StatusClosingBoundary Status = "closing_boundaries"
	StatusVerifyingRelief Status = "verifying_relief"
	StatusCompleted       Status = "completed"
	StatusFailed          Status = "failed"
)

type Session struct {
	ID          string                 `json:"id"`
	SectionID   string                 `json:"section_id"`
	ZoneID      string                 `json:"zone_id"`
	Status      Status                 `json:"status"`
	Relief      valve.LifecycleProof   `json:"relief"`
	Boundaries  []valve.LifecycleProof `json:"boundaries"`
	Operations  map[string]string      `json:"operations"`
	Error       string                 `json:"error,omitempty"`
	StartedAt   time.Time              `json:"started_at"`
	CompletedAt time.Time              `json:"completed_at,omitempty"`
}

type Service struct {
	topology   *network.Topology
	state      *network.State
	relief     *valve.ReliefService
	boundaries *valve.BoundaryService
	journal    *valve.LifecycleJournal
	audit      *audit.Recorder
	mu         sync.RWMutex
	sessions   map[string]Session
}

func NewService(topology *network.Topology, state *network.State, relief *valve.ReliefService, boundaries *valve.BoundaryService, journal *valve.LifecycleJournal, recorder *audit.Recorder) *Service {
	return &Service{topology: topology, state: state, relief: relief, boundaries: boundaries, journal: journal, audit: recorder, sessions: make(map[string]Session)}
}

func (s *Service) Isolate(ctx context.Context, sectionID, actor string) (Session, error) {
	section, err := s.topology.Section(sectionID)
	if err != nil {
		return Session{}, err
	}
	session := Session{
		ID: uuid.NewString(), SectionID: sectionID, ZoneID: section.ZoneID,
		Status: StatusOpeningRelief, StartedAt: time.Now().UTC(),
		Boundaries: make([]valve.LifecycleProof, 0, len(section.BoundaryValves)),
		Operations: make(map[string]string),
	}
	s.save(session)
	if _, err := s.state.Update(section.ZoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolating
		return nil
	}); err != nil {
		return s.fail(session, err)
	}
	// 泄压阀生命周期：申请 → 打开 → 保持
	reliefOp, err := s.relief.Request(session.ID, section.ReliefValve)
	if err != nil {
		return s.fail(session, err)
	}
	session.Operations[section.ReliefValve] = reliefOp.OperationID
	if session.Relief, err = s.relief.Open(ctx, reliefOp.OperationID); err != nil {
		return s.fail(session, err)
	}
	if session.Relief, err = s.relief.Hold(reliefOp.OperationID); err != nil {
		return s.fail(session, err)
	}
	session.Status = StatusClosingBoundary
	s.save(session)
	// 边界阀生命周期：申请 → 关闭到位 → 保持 → 关闭验证
	for _, valveID := range section.BoundaryValves {
		op, err := s.boundaries.Request(session.ID, valveID)
		if err != nil {
			return s.fail(session, err)
		}
		session.Operations[valveID] = op.OperationID
		if _, err := s.boundaries.Close(ctx, op.OperationID, session.Relief); err != nil {
			return s.fail(session, err)
		}
		if _, err := s.boundaries.Hold(op.OperationID); err != nil {
			return s.fail(session, err)
		}
		verified, err := s.boundaries.VerifyClosed(ctx, op.OperationID)
		if err != nil {
			return s.fail(session, err)
		}
		session.Boundaries = append(session.Boundaries, verified)
		s.save(session)
	}
	// 边界全部验证关闭后，关闭泄压阀并做关闭验证
	session.Status = StatusVerifyingRelief
	s.save(session)
	if session.Relief, err = s.relief.VerifyClosed(ctx, reliefOp.OperationID); err != nil {
		return s.fail(session, err)
	}
	if _, err := s.state.Update(section.ZoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolated
		return nil
	}); err != nil {
		return s.fail(session, err)
	}
	session.Status = StatusCompleted
	session.CompletedAt = time.Now().UTC()
	s.save(session)
	if err := s.audit.Accepted("isolation.complete", sectionID, actor, "section safely isolated", map[string]any{"session_id": session.ID}); err != nil {
		return session, err
	}
	return session, nil
}

func (s *Service) fail(session Session, cause error) (Session, error) {
	// 失败路径：同一会话内所有未到终态的阀操作都记录失败，阀位视为未确认
	_ = s.relief.FailSession(session.ID, cause.Error())
	_ = s.boundaries.FailSession(session.ID, cause.Error())
	session.Status = StatusFailed
	session.Error = cause.Error()
	session.CompletedAt = time.Now().UTC()
	s.save(session)
	_ = s.audit.Rejected("isolation.failed", session.SectionID, "system", cause.Error(), map[string]any{"session_id": session.ID})
	return session, fmt.Errorf("isolation %s: %w", session.ID, cause)
}

// Proofs returns every valve lifecycle proof recorded under one isolation
// session, so the whole operation chain can be correlated and audited.
func (s *Service) Proofs(sessionID string) ([]valve.LifecycleProof, error) {
	return s.journal.ForSession(sessionID)
}

func (s *Service) save(session Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[session.ID] = session
}

func (s *Service) Get(sessionID string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.sessions[sessionID]
	session.Boundaries = append([]valve.LifecycleProof(nil), session.Boundaries...)
	return session, ok
}

func (s *Service) List() []Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		session.Boundaries = append([]valve.LifecycleProof(nil), session.Boundaries...)
		result = append(result, session)
	}
	return result
}
