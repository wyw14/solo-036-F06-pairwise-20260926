package service

import (
	"errors"
	"testing"

	"github.com/wyw14/solo-036/internal/journal"
)

func TestQueryEventsRequiresPersistence(t *testing.T) {
	runtime := NewRuntime()
	if _, err := runtime.QueryEvents(journal.EventQuery{}); !errors.Is(err, ErrPersistenceNotConfigured) {
		t.Fatalf("got %v", err)
	}
}

func TestQueryEventsUsesPersistentStore(t *testing.T) {
	runtime := NewRuntime()
	if err := runtime.ConfigurePersistence(t.TempDir()); err != nil {
		t.Fatalf("configure persistence: %v", err)
	}
	page, err := runtime.QueryEvents(journal.EventQuery{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !page.Verified || page.HasMore || len(page.Entries) != 0 {
		t.Fatalf("got %+v", page)
	}
}
