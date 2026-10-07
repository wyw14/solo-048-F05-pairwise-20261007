package valve

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"heatpulse/internal/store"
)

// Phase is one stage of a valve operation's life cycle.
type Phase string

const (
	// PhaseRequested: an operation was applied for but not yet acknowledged.
	PhaseRequested Phase = "requested"
	// PhaseEstablished: the field layer acknowledged the valve reaching its target.
	PhaseEstablished Phase = "established"
	// PhaseHeld: the position was re-confirmed after establishment (relief stays open).
	PhaseHeld Phase = "held"
	// PhaseReleased: the valve was driven back to its resting position and that
	// position was independently verified. For a boundary valve this means the
	// valve is open again; relief proofs must never be reused past this point.
	PhaseReleased Phase = "released"
	// PhaseFailed: the operation could not be confirmed or its confirmation went stale.
	PhaseFailed Phase = "failed"
	// PhaseUnknown: no life-cycle evidence exists for the valve at all.
	PhaseUnknown Phase = "unknown"
)

// Direction describes which position an operation drives the valve toward.
type Direction string

const (
	DirectionOpening Direction = "opening"
	DirectionClosing Direction = "closing"
)

// Transition records a single life-cycle event. Every transition belonging to
// the same isolation session carries the same SessionID, so the request,
// establishment, holds, release verification and failures are correlatable.
type Transition struct {
	At        time.Time `json:"at"`
	Phase     Phase     `json:"phase"`
	Direction Direction `json:"direction"`
	Detail    string    `json:"detail,omitempty"`
}

// Lifecycle is the full, durable record of one valve operation.
type Lifecycle struct {
	ID          string    `json:"id"`
	ValveID     string    `json:"valve_id"`
	Kind        string    `json:"kind"` // "relief" | "boundary"
	SessionID   string    `json:"session_id"`
	Direction   Direction `json:"direction"`
	Phase       Phase     `json:"phase"`
	RequestedAt time.Time `json:"requested_at"`
	// ConfirmedAt is when the target position was first acknowledged. A zero
	// value means the position is unconfirmed and must never be trusted.
	ConfirmedAt time.Time    `json:"confirmed_at,omitempty"`
	LastHeldAt  time.Time    `json:"last_held_at,omitempty"`
	ReleasedAt  time.Time    `json:"released_at,omitempty"`
	FailedAt    time.Time    `json:"failed_at,omitempty"`
	Failure     string       `json:"failure,omitempty"`
	Transitions []Transition `json:"transitions"`
}

type lifecycleDocument struct {
	Next   int                   `json:"next"`
	Active map[string]*Lifecycle `json:"active"` // valve -> current lifecycle
	Past   []Lifecycle           `json:"past"`
}

// Registry durably records valve life cycles. All trust decisions ("is this
// valve safely in the position a session needs?") go through it.
type Registry struct {
	file       *store.File[lifecycleDocument]
	ttl        time.Duration
	sweepGrace time.Duration
	mu         sync.Mutex
	doc        lifecycleDocument
	loaded     bool
}

// NewRegistry builds a registry backed by <dataDir>/valve-lifecycles.json.
// holdTTL bounds how long an established position may go without a fresh hold
// confirmation; sweepGrace is the additional slack before a stale record is
// failed by the sweeper.
func NewRegistry(dataDir string, holdTTL time.Duration) *Registry {
	return &Registry{
		file:       store.NewFile[lifecycleDocument](filepath.Join(dataDir, "valve-lifecycles.json")),
		ttl:        holdTTL,
		sweepGrace: holdTTL,
	}
}

func (r *Registry) load() error {
	if r.loaded {
		return nil
	}
	doc, err := r.file.Load()
	if err != nil {
		return err
	}
	if doc.Active == nil {
		doc.Active = make(map[string]*Lifecycle)
	}
	if doc.Next == 0 {
		doc.Next = 1
	}
	r.doc = doc
	r.loaded = true
	return nil
}

// Recover is called once at process start. We cannot observe field positions
// after a restart, so every operation that never reached an independently
// verified release is failed rather than assumed safe. In particular a
// boundary valve whose closure was never released+verified is NOT reported as
// safely closed: the section stays "unknown / requires inspection".
func (r *Registry) Recover(now time.Time) (recovered []Lifecycle, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return nil, err
	}
	for valveID, lc := range r.doc.Active {
		if lc == nil {
			delete(r.doc.Active, valveID)
			continue
		}
		if lc.Phase == PhaseReleased {
			finished := *lc
			r.doc.Past = append(r.doc.Past, finished)
			delete(r.doc.Active, valveID)
			recovered = append(recovered, finished)
			continue
		}
		lc.Phase = PhaseFailed
		lc.FailedAt = now
		if lc.Failure == "" {
			lc.Failure = "unconfirmed valve position after restart; field verification required"
		}
		lc.Transitions = append(lc.Transitions, Transition{At: now, Phase: PhaseFailed, Direction: lc.Direction, Detail: "process restart"})
		finished := *lc
		r.doc.Past = append(r.doc.Past, finished)
		delete(r.doc.Active, valveID)
		recovered = append(recovered, finished)
	}
	if err := r.persistLocked(); err != nil {
		return nil, err
	}
	return recovered, nil
}

// Request opens a new lifecycle and supersedes any active lifecycle on the
// same valve (the prior one is failed — concurrent operations on one valve
// cannot both be trustworthy).
func (r *Registry) Request(valveID, kind, sessionID string, direction Direction, now time.Time) (Lifecycle, error) {
	if valveID == "" || sessionID == "" {
		return Lifecycle{}, fmt.Errorf("valve lifecycle requires valve and session identity")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return Lifecycle{}, err
	}
	if prior := r.doc.Active[valveID]; prior != nil && prior.Phase != PhaseReleased && prior.Phase != PhaseFailed {
		prior.Phase = PhaseFailed
		prior.FailedAt = now
		prior.Failure = fmt.Sprintf("superseded by lifecycle %s for session %s", r.idFor(r.doc.Next), sessionID)
		prior.Transitions = append(prior.Transitions, Transition{At: now, Phase: PhaseFailed, Direction: prior.Direction, Detail: "superseded"})
		r.doc.Past = append(r.doc.Past, *prior)
	}
	lc := Lifecycle{
		ID:          r.idFor(r.doc.Next),
		ValveID:     valveID,
		Kind:        kind,
		SessionID:   sessionID,
		Direction:   direction,
		Phase:       PhaseRequested,
		RequestedAt: now,
		Transitions: []Transition{{At: now, Phase: PhaseRequested, Direction: direction}},
	}
	r.doc.Next++
	r.doc.Active[valveID] = &lc
	if err := r.persistLocked(); err != nil {
		delete(r.doc.Active, valveID)
		return Lifecycle{}, err
	}
	return lc, nil
}

func (r *Registry) idFor(n int) string {
	return fmt.Sprintf("vlc-%06d", n)
}

func (r *Registry) persistLocked() error {
	const maxPast = 1000
	if excess := len(r.doc.Past) - maxPast; excess > 0 {
		r.doc.Past = append([]Lifecycle(nil), r.doc.Past[excess:]...)
	}
	_, err := r.file.Update(func(stored *lifecycleDocument) error {
		*stored = r.doc
		return nil
	})
	return err
}

// mutateLocked applies a state change to the active lifecycle of a valve and
// persists it. The change must not perform trust checks against time; callers
// do that via FreshLocked.
func (r *Registry) mutateLocked(valveID string, change func(*Lifecycle) error) (Lifecycle, error) {
	lc := r.doc.Active[valveID]
	if lc == nil {
		return Lifecycle{}, fmt.Errorf("no active lifecycle for valve %s", valveID)
	}
	if err := change(lc); err != nil {
		return Lifecycle{}, err
	}
	if err := r.persistLocked(); err != nil {
		return Lifecycle{}, err
	}
	return *lc, nil
}

// Establish marks the requested target position as acknowledged by the field.
func (r *Registry) Establish(valveID, sessionID string, now time.Time) (Lifecycle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return Lifecycle{}, err
	}
	return r.mutateLocked(valveID, func(lc *Lifecycle) error {
		if lc.SessionID != sessionID {
			return fmt.Errorf("lifecycle %s belongs to session %s, not %s", lc.ID, lc.SessionID, sessionID)
		}
		if lc.Phase == PhaseReleased || lc.Phase == PhaseFailed {
			return fmt.Errorf("lifecycle %s is %s and cannot be established", lc.ID, lc.Phase)
		}
		lc.Phase = PhaseEstablished
		lc.ConfirmedAt = now
		lc.LastHeldAt = now
		lc.Transitions = append(lc.Transitions, Transition{At: now, Phase: PhaseEstablished, Direction: lc.Direction, Detail: "field confirmation"})
		return nil
	})
}

// Hold records a fresh re-confirmation of an established position.
func (r *Registry) Hold(valveID, sessionID string, now time.Time) (Lifecycle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return Lifecycle{}, err
	}
	return r.mutateLocked(valveID, func(lc *Lifecycle) error {
		if lc.SessionID != sessionID {
			return fmt.Errorf("hold rejected: lifecycle %s belongs to session %s", lc.ID, lc.SessionID)
		}
		if lc.Phase != PhaseEstablished && lc.Phase != PhaseHeld {
			return fmt.Errorf("hold rejected: lifecycle %s is %s", lc.ID, lc.Phase)
		}
		// A lease that already lapsed cannot be renewed from here: that would
		// amount to claiming a position safe without a fresh field reading.
		// The lifecycle must be failed and re-verified in the field instead.
		if !lc.freshAt(now, r.ttl) {
			return fmt.Errorf("hold rejected: lifecycle %s confirmation is stale", lc.ID)
		}
		lc.Phase = PhaseHeld
		lc.LastHeldAt = now
		lc.Transitions = append(lc.Transitions, Transition{At: now, Phase: PhaseHeld, Direction: lc.Direction})
		return nil
	})
}

// Release records verified return to the resting position and archives the
// lifecycle. For a boundary (closing) operation the verification proves the
// valve is open again; for relief it proves the valve is closed again.
func (r *Registry) Release(valveID, sessionID string, now time.Time, detail string) (Lifecycle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return Lifecycle{}, err
	}
	lc, err := r.mutateLocked(valveID, func(lc *Lifecycle) error {
		if lc.SessionID != sessionID {
			return fmt.Errorf("release rejected: lifecycle %s belongs to session %s", lc.ID, lc.SessionID)
		}
		if lc.Phase == PhaseReleased {
			return fmt.Errorf("lifecycle %s already released", lc.ID)
		}
		if lc.Phase == PhaseFailed {
			return fmt.Errorf("lifecycle %s failed; safe release requires field inspection", lc.ID)
		}
		lc.Phase = PhaseReleased
		lc.ReleasedAt = now
		lc.Transitions = append(lc.Transitions, Transition{At: now, Phase: PhaseReleased, Direction: lc.Direction, Detail: detail})
		return nil
	})
	if err != nil {
		return Lifecycle{}, err
	}
	delete(r.doc.Active, valveID)
	r.doc.Past = append(r.doc.Past, lc)
	if err := r.persistLocked(); err != nil {
		return Lifecycle{}, err
	}
	return lc, nil
}

// Fail marks an active lifecycle failed and archives it. Fail is idempotent:
// re-failing an already failed/archived lifecycle returns its stored record.
func (r *Registry) Fail(valveID, sessionID string, now time.Time, reason string) (Lifecycle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return Lifecycle{}, err
	}
	if lc := r.doc.Active[valveID]; lc == nil {
		if finished, ok := r.findPast(valveID, sessionID); ok {
			return finished, nil
		}
		return Lifecycle{}, fmt.Errorf("no active lifecycle for valve %s", valveID)
	} else {
		lc, err := r.mutateLocked(valveID, func(lc *Lifecycle) error {
			if lc.SessionID != sessionID {
				return fmt.Errorf("fail rejected: lifecycle %s belongs to session %s", lc.ID, lc.SessionID)
			}
			if lc.Phase == PhaseFailed || lc.Phase == PhaseReleased {
				return nil
			}
			lc.Phase = PhaseFailed
			lc.FailedAt = now
			lc.Failure = reason
			lc.Transitions = append(lc.Transitions, Transition{At: now, Phase: PhaseFailed, Direction: lc.Direction, Detail: reason})
			return nil
		})
		if err != nil {
			return Lifecycle{}, err
		}
		delete(r.doc.Active, valveID)
		r.doc.Past = append(r.doc.Past, lc)
		if err := r.persistLocked(); err != nil {
			return Lifecycle{}, err
		}
		return lc, nil
	}
}

func (r *Registry) findPast(valveID, sessionID string) (Lifecycle, bool) {
	for i := len(r.doc.Past) - 1; i >= 0; i-- {
		if r.doc.Past[i].ValveID == valveID && r.doc.Past[i].SessionID == sessionID {
			return r.doc.Past[i], true
		}
	}
	return Lifecycle{}, false
}

// FreshLocked reports whether the lifecycle's position is currently confirmed
// and its most recent hold is within the lease TTL. Caller must hold r.mu.
func (lc *Lifecycle) freshAt(now time.Time, ttl time.Duration) bool {
	if lc.ConfirmedAt.IsZero() {
		return false
	}
	last := lc.ConfirmedAt
	if lc.LastHeldAt.After(last) {
		last = lc.LastHeldAt
	}
	return !last.Add(ttl).Before(now)
}

// ActiveLifecycle returns the active lifecycle for a valve in a session.
func (r *Registry) ActiveLifecycle(valveID, sessionID string) (Lifecycle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return Lifecycle{}, false
	}
	lc := r.doc.Active[valveID]
	if lc == nil || lc.SessionID != sessionID {
		return Lifecycle{}, false
	}
	return *lc, true
}

// anySession matches an active lifecycle regardless of owning session.
const anySession = "*"

// TrustedActive returns the active lifecycle only if it is in the given phase
// family, belongs to the session, and its confirmation is still fresh.
func (r *Registry) TrustedActive(valveID, sessionID string, phases ...Phase) (Lifecycle, bool) {
	return r.trustedAt(valveID, sessionID, time.Now().UTC(), phases...)
}

func (r *Registry) trustedAt(valveID, sessionID string, now time.Time, phases ...Phase) (Lifecycle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return Lifecycle{}, false
	}
	lc := r.doc.Active[valveID]
	if lc == nil || (sessionID != anySession && lc.SessionID != sessionID) {
		return Lifecycle{}, false
	}
	matched := false
	for _, p := range phases {
		if lc.Phase == p {
			matched = true
			break
		}
	}
	if !matched || !lc.freshAt(time.Now().UTC(), r.ttl) {
		return Lifecycle{}, false
	}
	return *lc, true
}

// Sweep fails active lifecycles whose hold lease has expired (with grace),
// returning the records it failed. A stale hold means we no longer have
// evidence the valve is where we need it — the position becomes untrusted,
// never silently "safe".
func (r *Registry) Sweep(now time.Time) ([]Lifecycle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return nil, err
	}
	var failed []Lifecycle
	for valveID, lc := range r.doc.Active {
		if lc.Phase != PhaseEstablished && lc.Phase != PhaseHeld {
			continue
		}
		if lc.freshAt(now, r.ttl+r.sweepGrace) {
			continue
		}
		lc.Phase = PhaseFailed
		lc.FailedAt = now
		lc.Failure = "hold confirmation timed out; valve position unconfirmed"
		lc.Transitions = append(lc.Transitions, Transition{At: now, Phase: PhaseFailed, Direction: lc.Direction, Detail: "hold timeout"})
		r.doc.Past = append(r.doc.Past, *lc)
		delete(r.doc.Active, valveID)
		failed = append(failed, *lc)
	}
	if len(failed) > 0 {
		if err := r.persistLocked(); err != nil {
			return nil, err
		}
	}
	return failed, nil
}

// History returns recorded lifecycles, newest first, optionally filtered by
// session ID (empty sessionID returns everything).
func (r *Registry) History(sessionID string, limit int) []Lifecycle {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		return nil
	}
	result := make([]Lifecycle, 0, len(r.doc.Past)+len(r.doc.Active))
	for _, lc := range r.doc.Active {
		if sessionID == "" || lc.SessionID == sessionID {
			result = append(result, *lc)
		}
	}
	for i := len(r.doc.Past) - 1; i >= 0; i-- {
		if sessionID == "" || r.doc.Past[i].SessionID == sessionID {
			result = append(result, r.doc.Past[i])
		}
	}
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result
}
