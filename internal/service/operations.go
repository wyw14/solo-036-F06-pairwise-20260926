package service

import (
	"errors"
	"fmt"
	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/run"
	"time"
)

func (r *Runtime) CreateOperation(station string) (model.Run, error) {
	created := r.Runs.Create(station)
	event := model.Event{ID: created.ID + "/loaded", RunID: created.ID, Kind: "state", Epoch: created.Epoch, Payload: string(created.State), At: time.Now().UTC()}
	if err := r.appendEvent(event); err != nil {
		return model.Run{}, fmt.Errorf("journal: %w", err)
	}
	return created, nil
}
func (r *Runtime) Operations() []model.Run { return run.Active(r.Runs) }
func (r *Runtime) JournalHealthy() bool    { return journal.Healthy(r.Journal) }

func (r *Runtime) QueryJournalEvents(query journal.EventQuery) (journal.EventPage, error) {
	control := r.ensureControl()
	if control.Persistent == nil {
		return journal.EventPage{}, errors.New("persistent journal is not configured")
	}
	return control.Persistent.Query(query)
}
