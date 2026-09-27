package journal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

func testEvent(runID, kind string, at time.Time) model.Event {
	return model.Event{
		ID:      fmt.Sprintf("%s/%s/%d", runID, kind, at.UnixNano()),
		RunID:   runID,
		Kind:    kind,
		Payload: "payload",
		At:      at,
	}
}

func appendEvents(t *testing.T, store *FileStore, events ...model.Event) {
	t.Helper()
	for _, event := range events {
		if err := store.Append(event); err != nil {
			t.Fatalf("append event: %v", err)
		}
	}
}

func rewriteLog(t *testing.T, directory string, keep int, mutate func(index int, line string) string) {
	t.Helper()
	path := filepath.Join(directory, "events.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read event log: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if keep > len(lines) {
		t.Fatalf("keep %d exceeds %d log lines", keep, len(lines))
	}
	lines = lines[:keep]
	for i := range lines {
		if mutate != nil {
			lines[i] = mutate(i, lines[i])
		}
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o640); err != nil {
		t.Fatalf("rewrite event log: %v", err)
	}
}

func eventSequences(page EventPage) []int64 {
	sequences := make([]int64, 0, len(page.Events))
	for _, event := range page.Events {
		sequences = append(sequences, event.Sequence)
	}
	return sequences
}

func TestQueryFiltersByRunKindAndTimeRange(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	appendEvents(t, store,
		testEvent("run-1", "launch", base),
		testEvent("run-1", "tracking", base.Add(time.Minute)),
		testEvent("run-2", "tracking", base.Add(2*time.Minute)),
		testEvent("run-1", "tracking", base.Add(3*time.Minute)),
		testEvent("run-1", "complete", base.Add(4*time.Minute)),
	)

	page, err := store.Query(EventQuery{RunID: "run-1", Kind: "tracking"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !page.Verified || !page.Complete {
		t.Fatalf("expected verified complete page, got %+v", page)
	}
	if got := eventSequences(page); len(got) != 2 || got[0] != 2 || got[1] != 4 {
		t.Fatalf("run/kind filter returned sequences %v, want [2 4]", got)
	}
	if page.VerifiedThrough != 5 {
		t.Fatalf("verified through %d, want 5", page.VerifiedThrough)
	}

	page, err = store.Query(EventQuery{
		RunID: "run-1",
		From:  base.Add(time.Minute),
		To:    base.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := eventSequences(page); len(got) != 2 || got[0] != 2 || got[1] != 4 {
		t.Fatalf("inclusive time range returned sequences %v, want [2 4]", got)
	}
}

func TestQueryPaginationIsStableWhileAppending(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		appendEvents(t, store, testEvent("run-1", "tracking", base.Add(time.Duration(i)*time.Minute)))
	}

	seen := make(map[int64]int)
	var order []int64
	cursor := ""
	var lastVerifiedThrough int64
	for page := 0; ; page++ {
		if page > 10 {
			t.Fatal("pagination did not terminate")
		}
		result, err := store.Query(EventQuery{RunID: "run-1", Kind: "tracking", After: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if !result.Verified {
			t.Fatalf("page %d lost verification: %s", page, result.Corruption)
		}
		if result.VerifiedThrough < lastVerifiedThrough {
			t.Fatalf("verified position regressed: %d < %d", result.VerifiedThrough, lastVerifiedThrough)
		}
		lastVerifiedThrough = result.VerifiedThrough
		for _, event := range result.Events {
			seen[event.Sequence]++
			order = append(order, event.Sequence)
		}
		if len(order) == 4 {
			// More matching events arrive while the client is mid-pagination.
			appendEvents(t, store,
				testEvent("run-1", "tracking", base.Add(5*time.Minute)),
				testEvent("run-1", "tracking", base.Add(6*time.Minute)),
				testEvent("run-2", "tracking", base.Add(7*time.Minute)),
			)
		}
		if result.Complete {
			break
		}
		if result.NextCursor == "" {
			t.Fatalf("page %d incomplete but returned no cursor", page)
		}
		if result.NextCursor == cursor {
			t.Fatalf("page %d cursor did not advance", page)
		}
		cursor = result.NextCursor
	}

	if len(order) != 7 {
		t.Fatalf("collected %d events, want 7: %v", len(order), order)
	}
	for i, sequence := range order {
		if sequence != int64(i+1) {
			t.Fatalf("position %d has sequence %d, want %d (gap or reorder): %v", i, sequence, i+1, order)
		}
	}
	for sequence, count := range seen {
		if count != 1 {
			t.Fatalf("sequence %d returned %d times", sequence, count)
		}
	}
	if lastVerifiedThrough != 8 {
		t.Fatalf("verified through %d, want 8", lastVerifiedThrough)
	}
}

func TestQueryReportsCorruptionInsteadOfSkipping(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileStore(directory)
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		appendEvents(t, store, testEvent("run-1", "tracking", base.Add(time.Duration(i)*time.Minute)))
	}
	// Tamper with the third record; its checksum no longer matches.
	rewriteLog(t, directory, 5, func(index int, line string) string {
		if index != 2 {
			return line
		}
		return strings.Replace(line, `"Payload":"payload"`, `"Payload":"tampered"`, 1)
	})

	page, err := store.Query(EventQuery{RunID: "run-1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if page.Verified {
		t.Fatal("corrupted record was silently accepted")
	}
	if page.Complete {
		t.Fatal("corrupted journal reported as complete")
	}
	if !strings.Contains(page.Corruption, "sequence 3") {
		t.Fatalf("corruption should point at sequence 3, got %q", page.Corruption)
	}
	if got := eventSequences(page); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("expected verified prefix [1 2], got %v", got)
	}
	if page.VerifiedThrough != 2 {
		t.Fatalf("verified through %d, want 2", page.VerifiedThrough)
	}

	// Resuming from the returned cursor must hit the corruption again,
	// never skip past it and emit later records.
	resumed, err := store.Query(EventQuery{RunID: "run-1", After: page.NextCursor})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Verified {
		t.Fatal("resume skipped the corrupted record")
	}
	if len(resumed.Events) != 0 {
		t.Fatalf("resume emitted events past corruption: %v", eventSequences(resumed))
	}
}

func TestQueryReportsTruncation(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileStore(directory)
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		appendEvents(t, store, testEvent("run-1", "tracking", base.Add(time.Duration(i)*time.Minute)))
	}
	// Externally truncate the log; the remaining prefix still verifies cleanly.
	rewriteLog(t, directory, 2, nil)

	page, err := store.Query(EventQuery{RunID: "run-1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if page.Verified || page.Complete {
		t.Fatalf("truncated journal reported as verified/complete: %+v", page)
	}
	if !strings.Contains(page.Corruption, "tail") {
		t.Fatalf("corruption should mention the tail, got %q", page.Corruption)
	}
	if got := eventSequences(page); len(got) != 2 {
		t.Fatalf("expected surviving prefix [1 2], got %v", got)
	}
}

func TestQueryRejectsCursorFromReplacedJournal(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileStore(directory)
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		appendEvents(t, store, testEvent("run-1", "tracking", base.Add(time.Duration(i)*time.Minute)))
	}
	first, err := store.Query(EventQuery{RunID: "run-1", Limit: 2})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("expected a resume cursor")
	}

	// Replace the log with a different but internally valid chain.
	if err := os.Remove(filepath.Join(directory, "events.jsonl")); err != nil {
		t.Fatalf("remove event log: %v", err)
	}
	replacement, err := NewFileStore(directory)
	if err != nil {
		t.Fatalf("new replacement store: %v", err)
	}
	for i := 0; i < 3; i++ {
		appendEvents(t, replacement, testEvent("run-9", "state", base.Add(time.Duration(10+i)*time.Minute)))
	}

	page, err := replacement.Query(EventQuery{RunID: "run-1", After: first.NextCursor})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if page.Verified {
		t.Fatal("cursor from a replaced journal was accepted")
	}
	if !strings.Contains(page.Corruption, "cursor") {
		t.Fatalf("corruption should mention the cursor, got %q", page.Corruption)
	}
}

func TestQueryValidation(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	for _, query := range []EventQuery{
		{Limit: -1},
		{After: "not-a-cursor"},
		{After: "3:"},
		{From: base, To: base.Add(-time.Minute)},
	} {
		if _, err := store.Query(query); !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("query %+v: expected ErrInvalidQuery, got %v", query, err)
		}
	}

	page, err := store.Query(EventQuery{})
	if err != nil {
		t.Fatalf("query empty journal: %v", err)
	}
	if !page.Verified || !page.Complete || len(page.Events) != 0 {
		t.Fatalf("empty journal should return a verified empty page, got %+v", page)
	}
}
