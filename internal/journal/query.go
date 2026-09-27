package journal

import (
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

const (
	// DefaultEventQueryLimit is the page size used when a query does not
	// specify one.
	DefaultEventQueryLimit = 100
	// MaxEventQueryLimit caps the page size so one query cannot read the
	// whole log at once.
	MaxEventQueryLimit = 1000
)

// ErrInvalidQuery marks event queries the caller must fix, such as a
// negative cursor, an inverted time range, or a cursor beyond the end of
// the log.
var ErrInvalidQuery = errors.New("invalid event query")

// EventQuery selects a page of events from the durable log. The zero
// value returns the first page of the unfiltered log.
type EventQuery struct {
	RunID string    // match only this run; empty matches all runs
	Kind  string    // match only this event kind; empty matches all kinds
	From  time.Time // earliest event time, inclusive; zero means unbounded
	To    time.Time // latest event time, inclusive; zero means unbounded
	After int64     // resume cursor: only records with Sequence > After are considered
	Limit int       // page size; values <= 0 use DefaultEventQueryLimit, capped at MaxEventQueryLimit
}

// EventEntry is one verified record from the durable log.
type EventEntry struct {
	Sequence int64       `json:"sequence"`
	Checksum string      `json:"checksum"`
	Event    model.Event `json:"event"`
}

// EventPage is one page of query results.
//
// Next is the stable resume position: pass it as EventQuery.After to
// continue. The log is append-only with monotonic sequences, so records
// appended between pages are picked up by later pages without missing or
// repeating earlier ones.
//
// Verified reports that the checksum chain held from the start of the log
// through Next. When it is false the page stops at the last verified
// record instead of skipping the corrupted one: CorruptSequence
// identifies the first untrusted record and Corruption describes why.
type EventPage struct {
	Entries         []EventEntry `json:"entries"`
	Next            int64        `json:"next"`
	HasMore         bool         `json:"has_more"`
	Verified        bool         `json:"verified"`
	CorruptSequence int64        `json:"corrupt_sequence,omitempty"`
	Corruption      string       `json:"corruption,omitempty"`
}

// Query returns the page of durable events matching the filter. Every
// page re-verifies the checksum chain from the start of the log and
// cross-checks the durable tail, so tampering or truncation between pages
// is reported rather than silently paged over.
func (s *FileStore) Query(query EventQuery) (EventPage, error) {
	if query.After < 0 {
		return EventPage{}, fmt.Errorf("%w: after must not be negative", ErrInvalidQuery)
	}
	if !query.From.IsZero() && !query.To.IsZero() && query.From.After(query.To) {
		return EventPage{}, fmt.Errorf("%w: from is after to", ErrInvalidQuery)
	}
	limit := query.Limit
	if limit <= 0 {
		limit = DefaultEventQueryLimit
	}
	if limit > MaxEventQueryLimit {
		limit = MaxEventQueryLimit
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if query.After > s.sequence {
		return EventPage{}, fmt.Errorf("%w: cursor %d is beyond the end of the log at %d", ErrInvalidQuery, query.After, s.sequence)
	}
	records, err := s.scanRecords()
	var corruption *CorruptionError
	switch {
	case err == nil:
	case errors.As(err, &corruption):
		// Serve the verified prefix and report the break below.
	default:
		return EventPage{}, err
	}
	if corruption == nil {
		// The scanned tail must match the in-memory tail; a mismatch
		// means the file was truncated or carries uncommitted records
		// since the tail was established, and neither may be served as
		// a complete log.
		scannedTail := int64(0)
		if len(records) > 0 {
			scannedTail = records[len(records)-1].Sequence
		}
		if scannedTail != s.sequence {
			first := scannedTail + 1
			if s.sequence < scannedTail {
				first = s.sequence + 1
			}
			corruption = &CorruptionError{
				Sequence: first,
				Err:      fmt.Errorf("journal tail does not match durable log: scanned to %d, tail is %d", scannedTail, s.sequence),
			}
		}
	}
	if corruption != nil {
		kept := 0
		for kept < len(records) && records[kept].Sequence < corruption.Sequence {
			kept++
		}
		records = records[:kept]
	}

	page := EventPage{Entries: make([]EventEntry, 0), Verified: true}
	next := query.After
	for _, record := range records {
		if record.Sequence <= query.After {
			continue
		}
		next = record.Sequence
		if !query.matches(record.Event) {
			continue
		}
		page.Entries = append(page.Entries, EventEntry{
			Sequence: record.Sequence,
			Checksum: record.Checksum,
			Event:    record.Event,
		})
		if len(page.Entries) == limit {
			page.Next = record.Sequence
			page.HasMore = true
			return page, nil
		}
	}
	if corruption != nil {
		page.Verified = false
		page.CorruptSequence = corruption.Sequence
		page.Corruption = corruption.Error()
		page.Next = corruption.Sequence - 1
		return page, nil
	}
	page.Next = next
	return page, nil
}

func (q EventQuery) matches(event model.Event) bool {
	if q.RunID != "" && event.RunID != q.RunID {
		return false
	}
	if q.Kind != "" && event.Kind != q.Kind {
		return false
	}
	if !q.From.IsZero() && event.At.Before(q.From) {
		return false
	}
	if !q.To.IsZero() && event.At.After(q.To) {
		return false
	}
	return true
}
