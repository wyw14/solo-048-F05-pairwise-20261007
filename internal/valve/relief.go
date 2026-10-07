package valve

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ReliefService drives pressure-relief valves. Every open operation runs the
// full life cycle (request -> establish -> hold -> verified close) in the
// Registry, so a relief proof is only trustworthy while it is confirmed and
// its hold lease is fresh.
type ReliefService struct {
	registry *Registry
	delay    time.Duration
	mu       sync.Mutex
	driving  map[string]string // valve -> session currently driving the valve
}

func NewReliefService(registry *Registry, delay time.Duration) *ReliefService {
	return &ReliefService{registry: registry, delay: delay, driving: make(map[string]string)}
}

// Open requests relief-valve opening for an isolation session and waits for
// field establishment. Request is persisted before any I/O, so a timeout or a
// restart leaves a failed/unconfirmed record instead of a presumed-open valve.
func (s *ReliefService) Open(ctx context.Context, valveID, sessionID string) (Lifecycle, error) {
	if err := s.admit(valveID, sessionID); err != nil {
		return Lifecycle{}, err
	}
	defer s.releaseDriver(valveID)
	now := time.Now().UTC()
	if _, err := s.registry.Request(valveID, "relief", sessionID, DirectionOpening, now); err != nil {
		return Lifecycle{}, err
	}
	if s.delay < 0 {
		return s.fail(valveID, sessionID, "relief valve driver unavailable")
	}
	select {
	case <-ctx.Done():
		return s.fail(valveID, sessionID, fmt.Sprintf("relief open timed out: %v", ctx.Err()))
	case <-time.After(s.delay):
	}
	established, err := s.registry.Establish(valveID, sessionID, time.Now().UTC())
	if err != nil {
		return Lifecycle{}, err
	}
	return established, nil
}

// Hold re-confirms that an open relief valve is still open, renewing its
// lease. Isolation sessions that stay open past the TTL must keep calling it.
func (s *ReliefService) Hold(valveID, sessionID string) (Lifecycle, error) {
	return s.registry.Hold(valveID, sessionID, time.Now().UTC())
}

// VerifyClose drives the relief valve back closed and records a verified
// release. The valve is only considered safely closed once the field
// verification lands; a timeout fails the lifecycle instead.
func (s *ReliefService) VerifyClose(ctx context.Context, valveID, sessionID string) (Lifecycle, error) {
	if err := s.admit(valveID, sessionID); err != nil {
		return Lifecycle{}, err
	}
	defer s.releaseDriver(valveID)
	if s.delay < 0 {
		return s.fail(valveID, sessionID, "relief valve driver unavailable")
	}
	select {
	case <-ctx.Done():
		return s.fail(valveID, sessionID, fmt.Sprintf("relief close verification timed out: %v", ctx.Err()))
	case <-time.After(s.delay):
	}
	return s.registry.Release(valveID, sessionID, time.Now().UTC(), "relief valve closed and verified")
}

// SafeOpen reports whether the valve is currently proven open by a fresh
// confirmation. Anything unconfirmed, failed, stale or belonging to a
// completed session reports false.
func (s *ReliefService) SafeOpen(valveID string) bool {
	_, ok := s.registry.TrustedActive(valveID, anySession, PhaseEstablished, PhaseHeld)
	return ok
}

// View describes the current trusted position of a relief valve.
func (s *ReliefService) View(valveID string) PositionView {
	return viewOf(s.registry, valveID)
}

func (s *ReliefService) admit(valveID, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner := s.driving[valveID]; owner != "" && owner != sessionID {
		return fmt.Errorf("relief valve %s is being driven by session %s", valveID, owner)
	}
	s.driving[valveID] = sessionID
	return nil
}

func (s *ReliefService) releaseDriver(valveID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.driving, valveID)
}

func (s *ReliefService) fail(valveID, sessionID, reason string) (Lifecycle, error) {
	lifecycle, err := s.registry.Fail(valveID, sessionID, time.Now().UTC(), reason)
	if err != nil {
		return Lifecycle{}, err
	}
	return lifecycle, fmt.Errorf("%s", reason)
}
