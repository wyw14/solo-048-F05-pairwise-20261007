package isolation

import (
	"context"
	"fmt"
	"sync"

	"heatpulse/internal/hydraulic"
)

// Coordinator serializes leak-driven isolation per section. Active state is
// derived from the durable session store where possible, so a restart does not
// allow a second isolation session to be opened on a section whose prior
// session is still live or indeterminate.
type Coordinator struct {
	service  *Service
	mu       sync.Mutex
	starting map[string]bool
}

func NewCoordinator(service *Service) *Coordinator {
	return &Coordinator{service: service, starting: make(map[string]bool)}
}

func (c *Coordinator) FromIncident(ctx context.Context, incident hydraulic.LeakIncident, sectionID string) (Session, error) {
	if !incident.Confirmed {
		return Session{}, fmt.Errorf("leak incident %s is not confirmed", incident.ID)
	}
	c.mu.Lock()
	if c.starting[sectionID] {
		c.mu.Unlock()
		return Session{}, fmt.Errorf("section %s isolation is already starting", sectionID)
	}
	if session, ok := c.service.ActiveForSection(sectionID); ok {
		c.mu.Unlock()
		return session, fmt.Errorf("section %s already has isolation session %s in state %s", sectionID, session.ID, session.Status)
	}
	c.starting[sectionID] = true
	c.mu.Unlock()
	session, err := c.service.Isolate(ctx, sectionID, "leak-protection")
	c.mu.Lock()
	delete(c.starting, sectionID)
	c.mu.Unlock()
	return session, err
}

func (c *Coordinator) Active(sectionID string) (Session, bool) {
	return c.service.ActiveForSection(sectionID)
}
