package service

import (
	"errors"

	"github.com/wyw14/solo-036/internal/journal"
)

// ErrPersistenceNotConfigured reports a durable-journal query against a
// runtime that was started without a persistence directory.
var ErrPersistenceNotConfigured = errors.New("persistent journal is not configured")

// QueryEvents searches the durable journal by run, event kind, and time
// window, returning one page with a stable resume cursor and the
// checksum-chain verification status.
func (r *Runtime) QueryEvents(query journal.EventQuery) (journal.EventPage, error) {
	r.mu.Lock()
	control := r.control
	r.mu.Unlock()
	if control == nil || control.Persistent == nil {
		return journal.EventPage{}, ErrPersistenceNotConfigured
	}
	return control.Persistent.Query(query)
}
