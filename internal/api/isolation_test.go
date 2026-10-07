package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"heatpulse/internal/isolation"
)

func doJSON(t *testing.T, handler http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var envelope struct {
		Data  map[string]any `json:"data"`
		Error string         `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	if envelope.Error != "" {
		envelope.Data = map[string]any{"error": envelope.Error}
	}
	return rec.Code, envelope.Data
}

func TestIsolationLifecycleOverHTTPAndRestart(t *testing.T) {
	dir := t.TempDir()
	app, err := NewApp(dir)
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	defer app.Close(context.Background())
	handler := app.Handler()

	status, body := doJSON(t, handler, http.MethodPost, "/api/v1/sections/east-branch/isolate", map[string]string{"actor": "operator"})
	if status != http.StatusOK {
		t.Fatalf("isolate status=%d body=%v", status, body)
	}
	sessionID, _ := body["id"].(string)
	if sessionID == "" || body["status"] != string(isolation.StatusCompleted) {
		t.Fatalf("unexpected session: %v", body)
	}

	status, body = doJSON(t, handler, http.MethodGet, "/api/v1/isolation-sessions/"+sessionID, nil)
	if status != http.StatusOK {
		t.Fatalf("get session status=%d", status)
	}
	lifecycles, _ := body["lifecycles"].([]any)
	if len(lifecycles) != 3 { // relief + two boundaries, all correlated
		t.Fatalf("expected 3 correlated lifecycles, got %d", len(lifecycles))
	}
	for _, item := range lifecycles {
		lc, _ := item.(map[string]any)
		if lc["session_id"] != sessionID {
			t.Fatalf("lifecycle not correlated: %v", lc)
		}
	}

	// Restore: boundaries verified open, then relief verified closed.
	status, body = doJSON(t, handler, http.MethodPost, "/api/v1/isolation-sessions/"+sessionID+"/restore", map[string]string{"actor": "operator"})
	if status != http.StatusOK || body["status"] != string(isolation.StatusRestored) {
		t.Fatalf("restore status=%d body=%v", status, body)
	}

	// A restored session cannot be restored again.
	status, _ = doJSON(t, handler, http.MethodPost, "/api/v1/isolation-sessions/"+sessionID+"/restore", map[string]string{})
	if status != http.StatusConflict {
		t.Fatalf("double restore should conflict, got %d", status)
	}
}

func TestRestartInvalidatesIsolationProof(t *testing.T) {
	dir := t.TempDir()
	app1, err := NewApp(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, body := doJSON(t, app1.Handler(), http.MethodPost, "/api/v1/sections/west-loop/isolate", map[string]string{"actor": "operator"})
	sessionID, _ := body["id"].(string)
	if sessionID == "" {
		t.Fatalf("isolate failed: %v", body)
	}
	if !app1.Relief.SafeOpen("west-rv") || !app1.Boundaries.SafeClosed("west-bv-a") {
		t.Fatal("pre-restart positions must be trusted")
	}
	if err := app1.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Simulate process restart on the same data directory.
	app2, err := NewApp(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer app2.Close(context.Background())
	if app2.Relief.SafeOpen("west-rv") {
		t.Fatal("after restart relief must not be presumed open")
	}
	if app2.Boundaries.SafeClosed("west-bv-a") {
		t.Fatal("after restart boundary must not be presumed safely closed")
	}
	session, ok := app2.Isolation.Get(sessionID)
	if !ok {
		t.Fatal("session should survive restart")
	}
	if session.Status != isolation.StatusIndeterminate {
		t.Fatalf("session must be indeterminate after restart, got %s: %s", session.Status, session.Error)
	}

	// Restore is refused: field inspection is required.
	status, body2 := doJSON(t, app2.Handler(), http.MethodPost, "/api/v1/isolation-sessions/"+sessionID+"/restore", map[string]string{})
	if status != http.StatusConflict {
		t.Fatalf("restore of indeterminate session must conflict, got %d: %v", status, body2)
	}

	// Starting a new isolation is also blocked while the section is indeterminate.
	status, body2 = doJSON(t, app2.Handler(), http.MethodPost, "/api/v1/sections/west-loop/isolate", map[string]string{"actor": "operator"})
	if status != http.StatusConflict {
		t.Fatalf("re-isolation must conflict while indeterminate, got %d: %v", status, body2)
	}
}

func TestTimeoutFailsReliefWithoutPresumingOpen(t *testing.T) {
	dir := t.TempDir()
	app, err := NewApp(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close(context.Background())

	// Cancel the context before the (2ms) driver actuation can establish the
	// valve. The recorded lifecycle must be failed, never presumed open.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := app.Relief.Open(ctx, "east-rv", "manual-session"); err == nil {
		t.Fatal("canceled open must fail")
	}
	if app.Relief.SafeOpen("east-rv") {
		t.Fatal("canceled relief open must never be treated as safely open")
	}
}
