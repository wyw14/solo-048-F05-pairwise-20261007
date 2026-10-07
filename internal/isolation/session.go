package isolation

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/network"
	"heatpulse/internal/store"
	"heatpulse/internal/valve"
)

type Status string

const (
	StatusOpeningRelief   Status = "opening_relief"
	StatusClosingBoundary Status = "closing_boundaries"
	StatusCompleted       Status = "completed"
	StatusRestored        Status = "restored"
	StatusFailed          Status = "failed"
	// StatusIndeterminate means the section may be isolated but the evidence
	// was invalidated (hold timeout or process restart). Valve positions must
	// not be presumed safe; field inspection is required.
	StatusIndeterminate Status = "indeterminate"
)

type Session struct {
	ID          string            `json:"id"`
	SectionID   string            `json:"section_id"`
	ZoneID      string            `json:"zone_id"`
	Actor       string            `json:"actor"`
	Status      Status            `json:"status"`
	Relief      valve.Lifecycle   `json:"relief"`
	Boundaries  []valve.Lifecycle `json:"boundaries"`
	Error       string            `json:"error,omitempty"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt time.Time         `json:"completed_at,omitempty"`
	RestoredAt  time.Time         `json:"restored_at,omitempty"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type sessionDocument struct {
	Sessions map[string]Session `json:"sessions"`
}

type Service struct {
	topology   *network.Topology
	state      *network.State
	registry   *valve.Registry
	relief     *valve.ReliefService
	boundaries *valve.BoundaryService
	audit      *audit.Recorder
	file       *store.File[sessionDocument]
	holdTTL    time.Duration

	stopSweeper context.CancelFunc
	sweeperDone chan struct{}
}

func NewService(dataDir string, topology *network.Topology, state *network.State, registry *valve.Registry,
	relief *valve.ReliefService, boundaries *valve.BoundaryService, recorder *audit.Recorder, holdTTL time.Duration) *Service {
	return &Service{
		topology: topology, state: state, registry: registry, relief: relief, boundaries: boundaries,
		audit: recorder, holdTTL: holdTTL,
		file: store.NewFile[sessionDocument](filepath.Join(dataDir, "isolation-sessions.json")),
	}
}

// Recover must run once at startup after the valve registry has been built.
// The registry recovery has already invalidated unconfirmed field positions;
// here every session that had not been restored is reconciled with that. A
// session that had completed isolation becomes indeterminate rather than
// "still safely isolated", because a restart removed all live proof.
func (s *Service) Recover() error {
	recovered, err := s.registry.Recover(time.Now().UTC())
	if err != nil {
		return fmt.Errorf("recover valve lifecycles: %w", err)
	}
	failures := make(map[string]string)
	for _, lc := range recovered {
		if lc.Phase == valve.PhaseFailed {
			failures[lc.SessionID] = lc.Failure
		}
	}
	if len(failures) == 0 {
		return nil
	}
	_, err = s.file.Update(func(doc *sessionDocument) error {
		if doc.Sessions == nil {
			return nil
		}
		for sessionID, reason := range failures {
			session, ok := doc.Sessions[sessionID]
			if !ok {
				continue
			}
			if session.Status == StatusRestored || session.Status == StatusFailed || session.Status == StatusIndeterminate {
				continue
			}
			if session.Status == StatusCompleted {
				session.Status = StatusIndeterminate
				session.Error = "valve proof invalidated after restart: " + reason
			} else {
				session.Status = StatusFailed
				session.Error = "interrupted by restart before isolation completed: " + reason
			}
			session.UpdatedAt = time.Now().UTC()
			doc.Sessions[sessionID] = session
			_ = s.audit.Rejected("isolation.recovered", session.SectionID, "system", session.Error,
				map[string]any{"session_id": session.ID, "status": string(session.Status)})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("recover isolation sessions: %w", err)
	}
	return nil
}

// StartSweeper renews hold confirmations for completed sessions and fails any
// lifecycle whose lease has lapsed. When a renewal fails the session becomes
// indeterminate: its valve positions are no longer considered proven.
func (s *Service) StartSweeper() {
	ctx, cancel := context.WithCancel(context.Background())
	s.stopSweeper = cancel
	s.sweeperDone = make(chan struct{})
	interval := s.holdTTL / 2
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		defer close(s.sweeperDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.maintain()
			}
		}
	}()
}

func (s *Service) StopSweeper() {
	if s.stopSweeper != nil {
		s.stopSweeper()
	}
	if s.sweeperDone != nil {
		<-s.sweeperDone
	}
}

func (s *Service) maintain() {
	s.sweepAt(time.Now().UTC())
}

func (s *Service) sweepAt(now time.Time) {
	for _, session := range s.List() {
		if session.Status != StatusCompleted {
			continue
		}
		cascade, reason := false, ""
		if _, err := s.relief.Hold(session.Relief.ValveID, session.ID); err != nil {
			cascade, reason = true, "relief hold lost: "+err.Error()
		} else {
			for _, lc := range session.Boundaries {
				if _, err := s.boundaries.Hold(lc.ValveID, session.ID); err != nil {
					cascade, reason = true, "boundary hold lost: "+err.Error()
					break
				}
			}
		}
		if cascade {
			s.invalidateAndCascade(session, reason, now)
		}
	}
	failed, err := s.registry.Sweep(now)
	if err != nil {
		return
	}
	bySession := make(map[string][]valve.Lifecycle)
	for _, lc := range failed {
		bySession[lc.SessionID] = append(bySession[lc.SessionID], lc)
	}
	for sessionID, lifecycles := range bySession {
		session, ok := s.Get(sessionID)
		if !ok {
			continue
		}
		reason := "valve proof timed out"
		if len(lifecycles) > 0 {
			reason = "valve " + lifecycles[0].ValveID + " proof timed out: " + lifecycles[0].Failure
		}
		s.invalidateAndCascade(session, reason, now)
	}
}

// invalidateAndCascade fails the whole isolation configuration once one of its
// valve proofs is lost: a boundary whose own confirmation is still fresh is no
// longer reported as safely closed, because the relief path that made the
// isolation safe is gone. Every device then requires field verification.
func (s *Service) invalidateAndCascade(session Session, reason string, now time.Time) {
	if session.Status != StatusCompleted {
		return
	}
	valveIDs := make([]string, 0, len(session.Boundaries)+1)
	valveIDs = append(valveIDs, session.Relief.ValveID)
	for _, lc := range session.Boundaries {
		valveIDs = append(valveIDs, lc.ValveID)
	}
	cascadeReason := "cascaded: " + reason
	for _, valveID := range valveIDs {
		if _, err := s.registry.Fail(valveID, session.ID, now, cascadeReason); err != nil {
			return
		}
	}
	_ = s.invalidateAt(session.ID, reason, now)
}

func (s *Service) invalidate(sessionID, reason string) error {
	return s.invalidateAt(sessionID, reason, time.Now().UTC())
}

func (s *Service) invalidateAt(sessionID, reason string, now time.Time) error {
	_, err := s.file.Update(func(doc *sessionDocument) error {
		session, ok := doc.Sessions[sessionID]
		if !ok || session.Status != StatusCompleted {
			return nil
		}
		session.Status = StatusIndeterminate
		session.Error = reason
		session.UpdatedAt = now
		doc.Sessions[sessionID] = session
		return nil
	})
	if err == nil {
		_ = s.audit.Rejected("isolation.invalidated", sessionID, "system", reason, map[string]any{"session_id": sessionID})
	}
	return err
}

func (s *Service) Isolate(ctx context.Context, sectionID, actor string) (Session, error) {
	section, err := s.topology.Section(sectionID)
	if err != nil {
		return Session{}, err
	}
	if _, err := s.blockingSession(sectionID); err != nil {
		return Session{}, err
	}
	session := Session{
		ID: uuid.NewString(), SectionID: sectionID, ZoneID: section.ZoneID, Actor: actor,
		Status: StatusOpeningRelief, StartedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Boundaries: make([]valve.Lifecycle, 0, len(section.BoundaryValves)),
	}
	if err := s.persist(session); err != nil {
		return Session{}, err
	}
	if _, err := s.state.Update(section.ZoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolating
		return nil
	}); err != nil {
		return s.fail(session, err)
	}
	reliefProof, err := s.relief.Open(ctx, section.ReliefValve, session.ID)
	if err != nil {
		return s.fail(session, err)
	}
	session.Relief = reliefProof
	session.Status = StatusClosingBoundary
	if err := s.persist(session); err != nil {
		return s.fail(session, err)
	}
	for _, valveID := range section.BoundaryValves {
		boundaryProof, err := s.boundaries.Close(ctx, valveID, session.ID, section.ReliefValve)
		if err != nil {
			return s.fail(session, err)
		}
		session.Boundaries = append(session.Boundaries, boundaryProof)
		if err := s.persist(session); err != nil {
			return s.fail(session, err)
		}
	}
	if _, err := s.state.Update(section.ZoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolated
		return nil
	}); err != nil {
		return s.fail(session, err)
	}
	session.Status = StatusCompleted
	session.CompletedAt = time.Now().UTC()
	session.UpdatedAt = session.CompletedAt
	if err := s.persist(session); err != nil {
		return s.fail(session, err)
	}
	if err := s.audit.Accepted("isolation.complete", sectionID, actor, "section safely isolated",
		map[string]any{"session_id": session.ID, "relief_lifecycle": reliefProof.ID}); err != nil {
		return session, err
	}
	return session, nil
}

// Restore ends a completed isolation: every boundary is driven open and
// independently verified, and only then is the relief valve verified closed.
// A session whose proofs are indeterminate cannot be auto-restored — the
// section requires field inspection.
func (s *Service) Restore(ctx context.Context, sessionID, actor string) (Session, error) {
	session, ok := s.Get(sessionID)
	if !ok {
		return Session{}, fmt.Errorf("isolation session %s not found", sessionID)
	}
	switch session.Status {
	case StatusRestored:
		return session, fmt.Errorf("session %s already restored", sessionID)
	case StatusIndeterminate:
		return session, fmt.Errorf("session %s valve positions are unconfirmed; field inspection required before restore", sessionID)
	case StatusFailed:
		return session, fmt.Errorf("session %s failed; field inspection required", sessionID)
	case StatusCompleted:
	default:
		return session, fmt.Errorf("session %s is %s and cannot be restored", sessionID, session.Status)
	}
	for i := range session.Boundaries {
		released, err := s.boundaries.VerifyOpen(ctx, session.Boundaries[i].ValveID, sessionID)
		if err != nil {
			return s.fail(session, fmt.Errorf("verify boundary %s open: %w", session.Boundaries[i].ValveID, err))
		}
		session.Boundaries[i] = released
	}
	reliefClosed, err := s.relief.VerifyClose(ctx, session.Relief.ValveID, sessionID)
	if err != nil {
		return s.fail(session, err)
	}
	session.Relief = reliefClosed
	session.Status = StatusRestored
	session.RestoredAt = time.Now().UTC()
	session.UpdatedAt = session.RestoredAt
	if err := s.persist(session); err != nil {
		return session, err
	}
	if _, err := s.state.Update(session.ZoneID, func(zone *network.Zone) error {
		if zone.Phase == network.PhaseIsolated || zone.Phase == network.PhaseIsolating {
			zone.Phase = network.PhaseRecovering
		}
		return nil
	}); err != nil {
		return session, err
	}
	if err := s.audit.Accepted("isolation.restored", session.SectionID, actor,
		"boundaries verified open and relief verified closed",
		map[string]any{"session_id": sessionID, "relief_lifecycle": reliefClosed.ID}); err != nil {
		return session, err
	}
	return session, nil
}

// blockingSession returns an error if the section currently has a session
// whose valve positions must not be disturbed.
func (s *Service) blockingSession(sectionID string) (Session, error) {
	for _, session := range s.List() {
		if session.SectionID != sectionID {
			continue
		}
		switch session.Status {
		case StatusOpeningRelief, StatusClosingBoundary, StatusCompleted, StatusIndeterminate:
			return session, fmt.Errorf("section %s has a %s isolation session %s", sectionID, session.Status, session.ID)
		}
	}
	return Session{}, nil
}

func (s *Service) fail(session Session, cause error) (Session, error) {
	session.Status = StatusFailed
	session.Error = cause.Error()
	session.CompletedAt = time.Now().UTC()
	session.UpdatedAt = session.CompletedAt
	_ = s.persist(session)
	if session.Relief.ValveID != "" {
		_, _ = s.registry.Fail(session.Relief.ValveID, session.ID, time.Now().UTC(), "session failed: "+cause.Error())
	}
	for _, lc := range session.Boundaries {
		_, _ = s.registry.Fail(lc.ValveID, session.ID, time.Now().UTC(), "session failed: "+cause.Error())
	}
	_ = s.audit.Rejected("isolation.failed", session.SectionID, "system", cause.Error(), map[string]any{"session_id": session.ID})
	return session, fmt.Errorf("isolation %s: %w", session.ID, cause)
}

func (s *Service) persist(session Session) error {
	_, err := s.file.Update(func(doc *sessionDocument) error {
		if doc.Sessions == nil {
			doc.Sessions = make(map[string]Session)
		}
		doc.Sessions[session.ID] = session
		return nil
	})
	return err
}

func (s *Service) Get(sessionID string) (Session, bool) {
	doc, err := s.file.Load()
	if err != nil {
		return Session{}, false
	}
	session, ok := doc.Sessions[sessionID]
	if ok {
		session.Boundaries = append([]valve.Lifecycle(nil), session.Boundaries...)
	}
	return session, ok
}

func (s *Service) List() []Session {
	doc, err := s.file.Load()
	if err != nil {
		return nil
	}
	result := make([]Session, 0, len(doc.Sessions))
	for _, session := range doc.Sessions {
		session.Boundaries = append([]valve.Lifecycle(nil), session.Boundaries...)
		result = append(result, session)
	}
	return result
}

// ForSection returns sessions for a section, newest started first.
func (s *Service) ForSection(sectionID string) []Session {
	all := s.List()
	result := make([]Session, 0, len(all))
	for _, session := range all {
		if session.SectionID == sectionID {
			result = append(result, session)
		}
	}
	return result
}

// ActiveForSection returns the live session for a section, if any. A completed
// isolation counts as live until restored; an indeterminate one counts as live
// until inspected; failed/restored sessions do not.
func (s *Service) ActiveForSection(sectionID string) (Session, bool) {
	var found Session
	for _, session := range s.ForSection(sectionID) {
		switch session.Status {
		case StatusOpeningRelief, StatusClosingBoundary, StatusCompleted, StatusIndeterminate:
			if found.ID == "" || session.StartedAt.After(found.StartedAt) {
				found = session
			}
		}
	}
	return found, found.ID != ""
}
