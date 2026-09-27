package journal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

const (
	// DefaultQueryLimit bounds a page when the caller does not ask for a size.
	DefaultQueryLimit = 100
	// MaxQueryLimit bounds a page even when the caller asks for more.
	MaxQueryLimit = 1000
)

// ErrInvalidQuery marks query parameters the caller must fix before retrying.
var ErrInvalidQuery = errors.New("invalid journal query")

// EventQuery filters the durable log by run, event kind, and event time
// (From/To are inclusive, zero means unbounded). After carries the cursor
// from a previous page; empty starts at the oldest record.
type EventQuery struct {
	RunID string
	Kind  string
	From  time.Time
	To    time.Time
	After string
	Limit int
}

// QueriedEvent is one verified record with its position in the checksum chain.
type QueriedEvent struct {
	Sequence int64       `json:"sequence"`
	Checksum string      `json:"checksum"`
	Event    model.Event `json:"event"`
}

// EventPage is one verified slice of the durable log. NextCursor resumes after
// the last scanned record; because records are append-only and numbered
// sequentially, resuming from it cannot skip or repeat records even while new
// events are appended between pages. Verified is false when the scan stopped
// at a corrupted record; Corruption describes it, and the record is never
// skipped to fake completeness.
type EventPage struct {
	Events          []QueriedEvent `json:"events"`
	NextCursor      string         `json:"next_cursor,omitempty"`
	Complete        bool           `json:"complete"`
	Verified        bool           `json:"verified"`
	VerifiedThrough int64          `json:"verified_through"`
	Corruption      string         `json:"corruption,omitempty"`
}

func encodeCursor(sequence int64, checksum string) string {
	if sequence < 1 {
		return ""
	}
	return strconv.FormatInt(sequence, 10) + ":" + checksum
}

func decodeCursor(cursor string) (int64, string, error) {
	if strings.TrimSpace(cursor) == "" {
		return 0, "", nil
	}
	sequencePart, checksum, found := strings.Cut(cursor, ":")
	if !found || checksum == "" {
		return 0, "", fmt.Errorf("%w: cursor %q is malformed", ErrInvalidQuery, cursor)
	}
	sequence, err := strconv.ParseInt(sequencePart, 10, 64)
	if err != nil || sequence < 1 {
		return 0, "", fmt.Errorf("%w: cursor %q is malformed", ErrInvalidQuery, cursor)
	}
	return sequence, checksum, nil
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

func (q EventQuery) pageLimit() (int, error) {
	switch {
	case q.Limit < 0:
		return 0, fmt.Errorf("%w: limit must not be negative", ErrInvalidQuery)
	case q.Limit == 0:
		return DefaultQueryLimit, nil
	case q.Limit > MaxQueryLimit:
		return MaxQueryLimit, nil
	default:
		return q.Limit, nil
	}
}

// Query reads the durable log one verified page at a time.
func (s *FileStore) Query(query EventQuery) (EventPage, error) {
	limit, err := query.pageLimit()
	if err != nil {
		return EventPage{}, err
	}
	if !query.From.IsZero() && !query.To.IsZero() && query.To.Before(query.From) {
		return EventPage{}, fmt.Errorf("%w: time range ends before it starts", ErrInvalidQuery)
	}
	afterSequence, afterChecksum, err := decodeCursor(query.After)
	if err != nil {
		return EventPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanLocked(afterSequence, afterChecksum, query, limit)
}

func (p *EventPage) corrupt(through int64, checksum string, reason string) EventPage {
	p.Verified = false
	p.Corruption = reason
	p.VerifiedThrough = through
	p.NextCursor = encodeCursor(through, checksum)
	return *p
}

// scanLocked verifies the chain record by record and collects matching events
// until the page fills, the log tail, or a corrupted record. The caller holds
// s.mu, so appends cannot interleave with the scan; across pages the
// sequence-numbered cursor keeps later scans aligned with earlier ones.
func (s *FileStore) scanLocked(afterSequence int64, afterChecksum string, query EventQuery, limit int) (EventPage, error) {
	page := EventPage{Events: make([]QueriedEvent, 0), Verified: true}
	file, err := os.Open(s.eventsPath)
	if errors.Is(err, os.ErrNotExist) {
		switch {
		case s.sequence != 0:
			page.Verified = false
			page.Corruption = fmt.Sprintf("journal file is missing but %d events were appended", s.sequence)
		case afterSequence != 0:
			page.Verified = false
			page.Corruption = fmt.Sprintf("cursor references sequence %d but the journal is empty", afterSequence)
		default:
			page.Complete = true
		}
		return page, nil
	}
	if err != nil {
		return EventPage{}, fmt.Errorf("open event log: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	var previous string
	var expectedSequence int64 = 1
	var lastSequence int64
	var lastChecksum string
	cursorSeen := afterSequence == 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var record FileRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return page.corrupt(lastSequence, lastChecksum, fmt.Sprintf("decode event record %d: %v", expectedSequence, err)), nil
		}
		if record.Sequence != expectedSequence {
			return page.corrupt(lastSequence, lastChecksum, fmt.Sprintf("event sequence gap: got %d want %d", record.Sequence, expectedSequence)), nil
		}
		if record.Previous != previous {
			return page.corrupt(lastSequence, lastChecksum, fmt.Sprintf("event chain mismatch at sequence %d", record.Sequence)), nil
		}
		expectedChecksum, err := eventChecksum(record.Sequence, record.Previous, record.Event)
		if err != nil {
			return EventPage{}, err
		}
		if record.Checksum != expectedChecksum {
			return page.corrupt(lastSequence, lastChecksum, fmt.Sprintf("event checksum mismatch at sequence %d", record.Sequence)), nil
		}
		if record.Sequence == afterSequence {
			if record.Checksum != afterChecksum {
				return page.corrupt(lastSequence, lastChecksum, fmt.Sprintf("cursor checksum mismatch at sequence %d: the journal changed since the cursor was issued", record.Sequence)), nil
			}
			cursorSeen = true
		}
		previous = record.Checksum
		expectedSequence++
		lastSequence = record.Sequence
		lastChecksum = record.Checksum
		if record.Sequence > afterSequence && query.matches(record.Event) {
			page.Events = append(page.Events, QueriedEvent{Sequence: record.Sequence, Checksum: record.Checksum, Event: record.Event})
			if len(page.Events) == limit {
				page.NextCursor = encodeCursor(record.Sequence, record.Checksum)
				page.VerifiedThrough = record.Sequence
				page.Complete = record.Sequence >= s.sequence
				return page, nil
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return EventPage{}, fmt.Errorf("scan event log: %w", err)
	}
	page.VerifiedThrough = lastSequence
	page.NextCursor = encodeCursor(lastSequence, lastChecksum)
	switch {
	case !cursorSeen:
		page.Verified = false
		page.Corruption = fmt.Sprintf("cursor references sequence %d beyond journal tail %d", afterSequence, lastSequence)
	case lastSequence != s.sequence || lastChecksum != s.checksum:
		page.Verified = false
		page.Corruption = fmt.Sprintf("journal tail mismatch: log ends at sequence %d but %d events were appended", lastSequence, s.sequence)
	default:
		page.Complete = true
	}
	return page, nil
}
