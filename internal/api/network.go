package api

import (
	"fmt"
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

func (a *App) listIsolationSessions(w http.ResponseWriter, r *http.Request) {
	sessions := a.Isolation.List()
	if sectionID := r.URL.Query().Get("section_id"); sectionID != "" {
		sessions = a.Isolation.ForSection(sectionID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (a *App) getIsolationSession(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "sessionID")
	session, ok := a.Isolation.Get(sessionID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("isolation session %s not found", sessionID))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session":    session,
		"lifecycles": a.ValveRegistry.History(sessionID, 0),
	})
}

func (a *App) restoreIsolationSession(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	session, err := a.Isolation.Restore(r.Context(), chi.URLParam(r, "sessionID"), input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (a *App) overview(w http.ResponseWriter, _ *http.Request) {
	auditSummary, _ := a.AuditQuery.Summary(time.Now().Add(-24 * time.Hour))
	eventCount, _ := a.Events.Count()
	interlocks := make(map[string]any)
	pumpCommands := make(map[string]any)
	sections := make(map[string]any)
	valvePositions := make(map[string]any)
	for _, zone := range a.Network.List() {
		candidate, _ := a.Interlock.Snapshot(zone.ID)
		interlocks[zone.ID] = candidate
		pumpCommands[zone.ID] = a.Pumps.CommandsForZone(zone.ID)
		zoneSections := a.Topology.SectionsForZone(zone.ID)
		sections[zone.ID] = zoneSections
		for _, section := range zoneSections {
			valvePositions[section.ReliefValve] = a.Relief.View(section.ReliefValve)
			for _, boundaryID := range section.BoundaryValves {
				valvePositions[boundaryID] = a.Boundaries.View(boundaryID)
			}
		}
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
		"valve_positions":          valvePositions,
		"target_primary_admission": map[string]any{"open": primaryOpen, "proof": primaryAdmission},
	})
}
