package valve

import "time"

// PositionView is the only safe summary of a valve's current position. It
// carries the evidence behind the judgement explicitly: callers must not infer
// a safe position from a stored bool.
type PositionView struct {
	ValveID     string    `json:"valve_id"`
	Kind        string    `json:"kind"`
	SessionID   string    `json:"session_id,omitempty"`
	Phase       Phase     `json:"phase"`
	Open        bool      `json:"open"`
	Trusted     bool      `json:"trusted"`
	Confirmed   bool      `json:"confirmed"`
	Fresh       bool      `json:"fresh"`
	ConfirmedAt time.Time `json:"confirmed_at,omitempty"`
	LastHeldAt  time.Time `json:"last_held_at,omitempty"`
	Failure     string    `json:"failure,omitempty"`
}

func viewOf(registry *Registry, valveID string) PositionView {
	now := time.Now().UTC()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if err := registry.load(); err != nil {
		return PositionView{ValveID: valveID, Phase: PhaseFailed, Failure: "lifecycle store unavailable"}
	}
	if lc := registry.doc.Active[valveID]; lc != nil {
		view := PositionView{
			ValveID:     lc.ValveID,
			Kind:        lc.Kind,
			SessionID:   lc.SessionID,
			Phase:       lc.Phase,
			Confirmed:   !lc.ConfirmedAt.IsZero(),
			ConfirmedAt: lc.ConfirmedAt,
			LastHeldAt:  lc.LastHeldAt,
			Failure:     lc.Failure,
		}
		view.Fresh = lc.freshAt(now, registry.ttl)
		view.Open = lc.Kind == "relief" // relief established/held = open; boundary = closed
		view.Trusted = view.Confirmed && view.Fresh &&
			(lc.Phase == PhaseEstablished || lc.Phase == PhaseHeld)
		return view
	}
	// No active operation: the resting position can only be asserted from a
	// verified release. With no history at all the position is unknown.
	resting := PositionView{ValveID: valveID, Phase: PhaseUnknown}
	for i := len(registry.doc.Past) - 1; i >= 0; i-- {
		lc := registry.doc.Past[i]
		if lc.ValveID != valveID {
			continue
		}
		resting.Kind = lc.Kind
		resting.SessionID = lc.SessionID
		if lc.Phase == PhaseReleased {
			resting.Phase = PhaseReleased
			// Relief rests closed; a boundary rests open.
			resting.Open = lc.Kind == "boundary"
			resting.Trusted = true
			resting.Confirmed = true
		} else {
			resting.Phase = PhaseFailed
			resting.Failure = lc.Failure
		}
		break
	}
	return resting
}
