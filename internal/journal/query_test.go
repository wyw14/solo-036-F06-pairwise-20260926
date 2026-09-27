package journal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

func newTestStore(t *testing.T) *FileStore {
	t.Helper()
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	return store
}

func appendEvents(t *testing.T, store *FileStore, events ...model.Event) {
	t.Helper()
	for _, event := range events {
		if err := store.Append(event); err != nil {
			t.Fatalf("append event: %v", err)
		}
	}
}

func sequences(page EventPage) []int64 {
	result := make([]int64, 0, len(page.Entries))
	for _, entry := range page.Entries {
		result = append(result, entry.Sequence)
	}
	return result
}

func equalSequences(got []int64, want ...int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// rewriteLine rewrites one 1-based line of the store's event log, used to
// simulate on-disk corruption and tampering.
func rewriteLine(t *testing.T, store *FileStore, target int, mutate func(string) string) {
	t.Helper()
	data, err := os.ReadFile(store.eventsPath)
	if err != nil {
		t.Fatalf("read event log: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if target < 1 || target > len(lines) {
		t.Fatalf("line %d out of range %d", target, len(lines))
	}
	lines[target-1] = mutate(lines[target-1])
	if err := os.WriteFile(store.eventsPath, []byte(strings.Join(lines, "\n")+"\n"), 0o640); err != nil {
		t.Fatalf("rewrite event log: %v", err)
	}
}

func TestQueryFiltersByRunKindAndTime(t *testing.T) {
	store := newTestStore(t)
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	appendEvents(t, store,
		model.Event{RunID: "run-1", Kind: "launch", At: base},
		model.Event{RunID: "run-2", Kind: "launch", At: base.Add(time.Minute)},
		model.Event{RunID: "run-1", Kind: "tracking", At: base.Add(2 * time.Minute)},
		model.Event{RunID: "run-1", Kind: "complete", At: base.Add(3 * time.Minute)},
	)

	page, err := store.Query(EventQuery{RunID: "run-1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !equalSequences(sequences(page), 1, 3, 4) {
		t.Fatalf("run filter got %v", sequences(page))
	}
	if !page.Verified || page.HasMore || page.Next != 4 {
		t.Fatalf("unexpected page status: %+v", page)
	}

	page, err = store.Query(EventQuery{RunID: "run-1", Kind: "tracking"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !equalSequences(sequences(page), 3) {
		t.Fatalf("kind filter got %v", sequences(page))
	}

	page, err = store.Query(EventQuery{
		From: base.Add(30 * time.Second),
		To:   base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !equalSequences(sequences(page), 2, 3) {
		t.Fatalf("time range got %v", sequences(page))
	}

	// Bounds are inclusive.
	page, err = store.Query(EventQuery{From: base, To: base})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !equalSequences(sequences(page), 1) {
		t.Fatalf("inclusive bounds got %v", sequences(page))
	}
}

func TestQueryPagesWithoutGapsOrDuplicates(t *testing.T) {
	store := newTestStore(t)
	for i := 0; i < 10; i++ {
		appendEvents(t, store, model.Event{RunID: "run-1", Kind: "tracking"})
	}
	seen := make([]int64, 0, 10)
	var cursor int64
	for {
		page, err := store.Query(EventQuery{After: cursor, Limit: 4})
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if !page.Verified {
			t.Fatalf("page not verified: %s", page.Corruption)
		}
		seen = append(seen, sequences(page)...)
		cursor = page.Next
		if !page.HasMore {
			break
		}
	}
	if !equalSequences(seen, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) {
		t.Fatalf("paged union got %v", seen)
	}
	if cursor != 10 {
		t.Fatalf("final cursor got %d", cursor)
	}
}

func TestQueryPagesAcrossAppends(t *testing.T) {
	store := newTestStore(t)
	for i := 0; i < 5; i++ {
		appendEvents(t, store, model.Event{RunID: "run-1", Kind: "tracking"})
	}

	page, err := store.Query(EventQuery{Limit: 3})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !equalSequences(sequences(page), 1, 2, 3) || !page.HasMore || page.Next != 3 {
		t.Fatalf("first page got %v next %d", sequences(page), page.Next)
	}

	// Appends between pages must surface in later pages exactly once.
	appendEvents(t, store,
		model.Event{RunID: "run-1", Kind: "tracking"},
		model.Event{RunID: "run-1", Kind: "complete"},
	)

	page, err = store.Query(EventQuery{After: page.Next, Limit: 3})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !equalSequences(sequences(page), 4, 5, 6) || !page.HasMore || page.Next != 6 {
		t.Fatalf("second page got %v next %d", sequences(page), page.Next)
	}

	page, err = store.Query(EventQuery{After: page.Next, Limit: 3})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !equalSequences(sequences(page), 7) || page.HasMore || page.Next != 7 {
		t.Fatalf("third page got %v next %d", sequences(page), page.Next)
	}

	// Polling at the tail is a stable empty page.
	page, err = store.Query(EventQuery{After: page.Next})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(page.Entries) != 0 || page.HasMore || page.Next != 7 || !page.Verified {
		t.Fatalf("tail page got %+v", page)
	}
}

func TestQueryCursorAdvancesPastNonMatchingRecords(t *testing.T) {
	store := newTestStore(t)
	appendEvents(t, store,
		model.Event{RunID: "run-1", Kind: "launch"},
		model.Event{RunID: "run-2", Kind: "launch"},
		model.Event{RunID: "run-2", Kind: "tracking"},
	)
	page, err := store.Query(EventQuery{RunID: "run-1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !equalSequences(sequences(page), 1) {
		t.Fatalf("got %v", sequences(page))
	}
	// The cursor sits at the scanned tail so future polls do not rescan.
	if page.Next != 3 || page.HasMore {
		t.Fatalf("cursor got %d has_more %v", page.Next, page.HasMore)
	}
}

func TestQueryReportsCorruptionWithoutSkipping(t *testing.T) {
	store := newTestStore(t)
	for i := 0; i < 6; i++ {
		appendEvents(t, store, model.Event{RunID: "run-1", Kind: "tracking", Payload: fmt.Sprintf("payload-%d", i+1)})
	}
	rewriteLine(t, store, 4, func(line string) string {
		return strings.Replace(line, "payload-4", "payload-X", 1)
	})

	page, err := store.Query(EventQuery{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if page.Verified {
		t.Fatalf("corrupt log reported verified")
	}
	if page.CorruptSequence != 4 || page.Corruption == "" {
		t.Fatalf("corruption status got %+v", page)
	}
	// The verified prefix is served; records at and after the corruption
	// are not skipped over and silently served.
	if !equalSequences(sequences(page), 1, 2, 3) {
		t.Fatalf("entries got %v", sequences(page))
	}
	if page.HasMore || page.Next != 3 {
		t.Fatalf("cursor got next %d has_more %v", page.Next, page.HasMore)
	}

	// Continuing from the cursor hits the same corruption again instead
	// of paging past it.
	page, err = store.Query(EventQuery{After: page.Next})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if page.Verified || page.CorruptSequence != 4 || len(page.Entries) != 0 {
		t.Fatalf("second page got %+v", page)
	}
}

func TestQueryReportsUndecodableRecord(t *testing.T) {
	store := newTestStore(t)
	appendEvents(t, store,
		model.Event{RunID: "run-1", Kind: "launch"},
		model.Event{RunID: "run-1", Kind: "complete"},
	)
	rewriteLine(t, store, 2, func(line string) string {
		return line[:len(line)/2]
	})
	page, err := store.Query(EventQuery{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if page.Verified || page.CorruptSequence != 2 {
		t.Fatalf("got %+v", page)
	}
	if !equalSequences(sequences(page), 1) {
		t.Fatalf("entries got %v", sequences(page))
	}
}

func TestQueryDetectsTamperingBetweenPages(t *testing.T) {
	store := newTestStore(t)
	for i := 0; i < 4; i++ {
		appendEvents(t, store, model.Event{RunID: "run-1", Kind: "tracking"})
	}
	first, err := store.Query(EventQuery{Limit: 2})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !first.Verified || first.Next != 2 {
		t.Fatalf("first page got %+v", first)
	}

	// Tamper with a record the first page already served; the next page
	// must notice because verification always starts at sequence 1.
	rewriteLine(t, store, 2, func(line string) string {
		return strings.Replace(line, `"tracking"`, `"launch"`, 1)
	})
	second, err := store.Query(EventQuery{After: first.Next})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if second.Verified || second.CorruptSequence != 2 {
		t.Fatalf("second page got %+v", second)
	}
}

func TestQueryDetectsExternalTruncation(t *testing.T) {
	store := newTestStore(t)
	for i := 0; i < 5; i++ {
		appendEvents(t, store, model.Event{RunID: "run-1", Kind: "tracking"})
	}
	data, err := os.ReadFile(store.eventsPath)
	if err != nil {
		t.Fatalf("read event log: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if err := os.WriteFile(store.eventsPath, []byte(strings.Join(lines[:3], "\n")+"\n"), 0o640); err != nil {
		t.Fatalf("truncate event log: %v", err)
	}

	page, err := store.Query(EventQuery{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if page.Verified {
		t.Fatalf("truncated log reported verified")
	}
	if page.CorruptSequence != 4 || page.Next != 3 {
		t.Fatalf("got %+v", page)
	}
	if !equalSequences(sequences(page), 1, 2, 3) {
		t.Fatalf("entries got %v", sequences(page))
	}
}

func TestQueryValidation(t *testing.T) {
	store := newTestStore(t)
	appendEvents(t, store, model.Event{RunID: "run-1", Kind: "launch"})

	if _, err := store.Query(EventQuery{After: -1}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("negative after got %v", err)
	}
	if _, err := store.Query(EventQuery{After: 2}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("cursor beyond tail got %v", err)
	}
	now := time.Now().UTC()
	if _, err := store.Query(EventQuery{From: now, To: now.Add(-time.Minute)}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("inverted range got %v", err)
	}
}

func TestQueryEmptyLog(t *testing.T) {
	store := newTestStore(t)
	page, err := store.Query(EventQuery{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !page.Verified || page.HasMore || page.Next != 0 || len(page.Entries) != 0 {
		t.Fatalf("got %+v", page)
	}
}

func TestQueryLimitClamp(t *testing.T) {
	store := newTestStore(t)
	for i := 0; i < 3; i++ {
		appendEvents(t, store, model.Event{RunID: "run-1", Kind: "tracking"})
	}
	page, err := store.Query(EventQuery{Limit: MaxEventQueryLimit * 2})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !equalSequences(sequences(page), 1, 2, 3) || page.HasMore {
		t.Fatalf("got %v has_more %v", sequences(page), page.HasMore)
	}
}

func TestQueryConcurrentAppends(t *testing.T) {
	store := newTestStore(t)
	const writers = 4
	const perWriter = 25
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				_ = store.Append(model.Event{RunID: fmt.Sprintf("run-%d", id), Kind: "tracking"})
			}
		}(w)
	}

	seen := make(map[int64]bool)
	var cursor int64
	collect := func() {
		for {
			page, err := store.Query(EventQuery{After: cursor, Limit: 7})
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if !page.Verified {
				t.Fatalf("page not verified: %s", page.Corruption)
			}
			for _, entry := range page.Entries {
				if entry.Sequence <= cursor {
					t.Fatalf("sequence %d not after cursor %d", entry.Sequence, cursor)
				}
				if seen[entry.Sequence] {
					t.Fatalf("duplicate sequence %d", entry.Sequence)
				}
				seen[entry.Sequence] = true
			}
			cursor = page.Next
			if !page.HasMore {
				return
			}
		}
	}
	collect() // page while appends are in flight
	wg.Wait()
	collect() // drain whatever landed after the first pass

	total := int64(writers * perWriter)
	if int64(len(seen)) != total {
		t.Fatalf("saw %d events, want %d", len(seen), total)
	}
	for seq := int64(1); seq <= total; seq++ {
		if !seen[seq] {
			t.Fatalf("missing sequence %d", seq)
		}
	}
}

func TestQuerySurvivesReopen(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileStore(directory)
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	appendEvents(t, store,
		model.Event{RunID: "run-1", Kind: "launch"},
		model.Event{RunID: "run-1", Kind: "complete"},
	)
	page, err := store.Query(EventQuery{Limit: 1})
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	reopened, err := NewFileStore(directory)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	resumed, err := reopened.Query(EventQuery{After: page.Next})
	if err != nil {
		t.Fatalf("resumed query: %v", err)
	}
	// The cursor is durable: it resumes correctly across restarts.
	if !equalSequences(sequences(resumed), 2) || resumed.HasMore || !resumed.Verified {
		t.Fatalf("resumed page got %+v", resumed)
	}
	if _, err := os.Stat(filepath.Join(directory, "events.jsonl")); err != nil {
		t.Fatalf("event log missing: %v", err)
	}
}
