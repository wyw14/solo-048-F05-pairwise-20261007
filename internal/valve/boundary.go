package valve

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BoundaryService drives section boundary valves. Closure is only permitted
// while the owning isolation session's relief valve is provably open (fresh
// hold), and every closure runs the durable life cycle. A boundary is never
// treated as safely closed just because a request was sent or an old proof is
// lying around: it needs field establishment within a fresh lease.
type BoundaryService struct {
	registry *Registry
	mu       sync.Mutex
	driving  map[string]string
}

func NewBoundaryService(registry *Registry) *BoundaryService {
	return &BoundaryService{registry: registry, driving: make(map[string]string)}
}

// actuationDelay is the simulated field-actuation time. In production this is
// replaced by driver I/O; its only purpose here is to make cancellation
// observable.
const actuationDelay = 2 * time.Millisecond

// Close requests boundary closure for an isolation session after
// re-verifying that the session's relief valve is currently proven open with a
// fresh lease. Passing a stale, failed, cross-session or released relief proof
// is rejected. Timeout during actuation fails the boundary lifecycle rather
// than leaving a presumed-closed valve.
func (s *BoundaryService) Close(ctx context.Context, valveID, sessionID, reliefValveID string) (Lifecycle, error) {
	if err := s.requireReliefOpen(reliefValveID, sessionID); err != nil {
		return Lifecycle{}, err
	}
	if err := s.admit(valveID, sessionID); err != nil {
		return Lifecycle{}, err
	}
	defer s.releaseDriver(valveID)
	if _, err := s.registry.Request(valveID, "boundary", sessionID, DirectionClosing, time.Now().UTC()); err != nil {
		return Lifecycle{}, err
	}
	// Re-check after the request has been durably recorded: the relief could
	// have failed or gone stale in the meantime.
	if err := s.requireReliefOpen(reliefValveID, sessionID); err != nil {
		return s.fail(valveID, sessionID, err.Error())
	}
	select {
	case <-ctx.Done():
		return s.fail(valveID, sessionID, fmt.Sprintf("boundary close timed out: %v", ctx.Err()))
	case <-time.After(actuationDelay):
	}
	// Establishment must still be backed by a live relief path.
	if err := s.requireReliefOpen(reliefValveID, sessionID); err != nil {
		return s.fail(valveID, sessionID, err.Error())
	}
	established, err := s.registry.Establish(valveID, sessionID, time.Now().UTC())
	if err != nil {
		return Lifecycle{}, err
	}
	return established, nil
}

// Hold re-confirms a closed boundary, renewing its lease.
func (s *BoundaryService) Hold(valveID, sessionID string) (Lifecycle, error) {
	return s.registry.Hold(valveID, sessionID, time.Now().UTC())
}

// VerifyOpen drives the boundary back open and records a verified release;
// only after this is the section safe to re-pressurize.
func (s *BoundaryService) VerifyOpen(ctx context.Context, valveID, sessionID string) (Lifecycle, error) {
	if err := s.admit(valveID, sessionID); err != nil {
		return Lifecycle{}, err
	}
	defer s.releaseDriver(valveID)
	select {
	case <-ctx.Done():
		return s.fail(valveID, sessionID, fmt.Sprintf("boundary open verification timed out: %v", ctx.Err()))
	case <-time.After(actuationDelay):
	}
	return s.registry.Release(valveID, sessionID, time.Now().UTC(), "boundary valve re-opened and verified")
}

// SafeClosed reports whether the boundary is currently proven closed by a
// fresh confirmation.
func (s *BoundaryService) SafeClosed(valveID string) bool {
	_, ok := s.registry.TrustedActive(valveID, anySession, PhaseEstablished, PhaseHeld)
	return ok
}

// View describes the current trusted position of a boundary valve.
func (s *BoundaryService) View(valveID string) PositionView {
	return viewOf(s.registry, valveID)
}

func (s *BoundaryService) requireReliefOpen(reliefValveID, sessionID string) error {
	if _, ok := s.registry.TrustedActive(reliefValveID, sessionID, PhaseEstablished, PhaseHeld); !ok {
		return fmt.Errorf("boundary closure blocked: relief valve %s is not proven open for session %s", reliefValveID, sessionID)
	}
	return nil
}

func (s *BoundaryService) admit(valveID, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner := s.driving[valveID]; owner != "" && owner != sessionID {
		return fmt.Errorf("boundary valve %s is being driven by session %s", valveID, owner)
	}
	s.driving[valveID] = sessionID
	return nil
}

func (s *BoundaryService) releaseDriver(valveID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.driving, valveID)
}

func (s *BoundaryService) fail(valveID, sessionID, reason string) (Lifecycle, error) {
	lifecycle, err := s.registry.Fail(valveID, sessionID, time.Now().UTC(), reason)
	if err != nil {
		return Lifecycle{}, err
	}
	return lifecycle, fmt.Errorf("%s", reason)
}
