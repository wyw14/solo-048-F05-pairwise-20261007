package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

func (a *App) listZones(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.Network.List())
}

func (a *App) setPumpStage(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Stage int    `json:"stage"`
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	zone, command, err := a.Pumps.SetStage(r.Context(), chi.URLParam(r, "zoneID"), input.Stage, input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"zone": zone, "command": command})
}

func (a *App) setBypass(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ValveID string `json:"valve_id"`
		Open    bool   `json:"open"`
		Actor   string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	zone, command, err := a.Valves.SetBypass(r.Context(), chi.URLParam(r, "zoneID"), input.ValveID, input.Open, input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"zone": zone, "command": command})
}

func (a *App) isolateSection(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	session, err := a.Isolation.Isolate(r.Context(), chi.URLParam(r, "sectionID"), input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (a *App) isolationProofs(w http.ResponseWriter, r *http.Request) {
	proofs, err := a.Isolation.Proofs(chi.URLParam(r, "sessionID"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, proofs)
}

func (a *App) overview(w http.ResponseWriter, _ *http.Request) {
	auditSummary, _ := a.AuditQuery.Summary(time.Now().Add(-24 * time.Hour))
	eventCount, _ := a.Events.Count()
	interlocks := make(map[string]any)
	pumpCommands := make(map[string]any)
	sections := make(map[string]any)
	for _, zone := range a.Network.List() {
		candidate, _ := a.Interlock.Snapshot(zone.ID)
		interlocks[zone.ID] = candidate
		pumpCommands[zone.ID] = a.Pumps.CommandsForZone(zone.ID)
		sections[zone.ID] = a.Topology.SectionsForZone(zone.ID)
	}
	stationDetails := make(map[string]any)
	for _, item := range a.Stations.List() {
		lastCommand, _ := a.StationCommands.Last(item.ID)
		stationDetails[item.ID] = map[string]any{
			"flow_receipts":         a.FlowReceipts.ForStation(item.ID),
			"last_command":          lastCommand,
			"dispatch_pending":      a.DispatchBuffer.Pending(item.ID),
			"antifreeze_cycles":     a.Circulation.Cycles(item.ID),
			"frost_valve_available": a.Frost.Available(item.ID + "-return"),
		}
	}
	primaryAdmission, primaryOpen := a.Primary.Admission("xincheng", "B")
	writeJSON(w, http.StatusOK, map[string]any{
		"zones": a.Network.List(), "stations": a.Stations.List(), "incidents": a.Protection.List(),
		"isolations": a.Isolation.List(), "plans": a.Planner.Plans(), "audit_summary": auditSummary,
		"event_count": eventCount, "hydraulic_threshold": a.Balancer.Threshold(),
		"interlocks": interlocks, "pump_commands": pumpCommands, "sections": sections,
		"station_details": stationDetails, "hydraulic_incidents": a.LeakDetector.Incidents(),
		"east_relief_open": a.Relief.IsOpen("east-rv"),
		"east_boundaries_closed": map[string]bool{
			"east-bv-a": a.Boundaries.IsClosed("east-bv-a"),
			"east-bv-b": a.Boundaries.IsClosed("east-bv-b"),
		},
		"east_boundaries_verified": map[string]bool{
			"east-bv-a": a.Boundaries.VerifiedClosed("east-bv-a"),
			"east-bv-b": a.Boundaries.VerifiedClosed("east-bv-b"),
		},
		"target_primary_admission": map[string]any{"open": primaryOpen, "proof": primaryAdmission},
	})
}
