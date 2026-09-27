package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/service"
)

func TestEventsWithoutPersistence(t *testing.T) {
	server := httptest.NewServer(NewRouter(service.NewRuntime()))
	defer server.Close()
	response, err := http.Get(server.URL + "/api/events")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("status got %d", response.StatusCode)
	}
}

func TestEventsRejectsBadParameters(t *testing.T) {
	runtime := service.NewRuntime()
	if err := runtime.ConfigurePersistence(t.TempDir()); err != nil {
		t.Fatalf("configure persistence: %v", err)
	}
	server := httptest.NewServer(NewRouter(runtime))
	defer server.Close()
	for _, query := range []string{"after=x", "limit=0", "from=not-a-time", "after=5"} {
		response, err := http.Get(server.URL + "/api/events?" + query)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: status got %d", query, response.StatusCode)
		}
	}
}

func TestEventsReturnsVerifiedPage(t *testing.T) {
	runtime := service.NewRuntime()
	if err := runtime.ConfigurePersistence(t.TempDir()); err != nil {
		t.Fatalf("configure persistence: %v", err)
	}
	server := httptest.NewServer(NewRouter(runtime))
	defer server.Close()
	response, err := http.Get(server.URL + "/api/events?limit=10")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status got %d", response.StatusCode)
	}
	var page journal.EventPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !page.Verified || page.HasMore || page.Next != 0 {
		t.Fatalf("got %+v", page)
	}
}
