package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/wyw14/solo-036/internal/journal"
)

func (h *Handler) JournalEvents(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	query := journal.EventQuery{
		RunID: values.Get("run_id"),
		Kind:  values.Get("kind"),
		After: values.Get("cursor"),
	}
	if value := values.Get("from"); value != "" {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.New("from must use RFC3339 format"))
			return
		}
		query.From = parsed.UTC()
	}
	if value := values.Get("to"); value != "" {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.New("to must use RFC3339 format"))
			return
		}
		query.To = parsed.UTC()
	}
	if value := values.Get("limit"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 {
			writeError(w, r, http.StatusBadRequest, errors.New("limit must be a positive integer"))
			return
		}
		query.Limit = limit
	}
	page, err := h.runtime.QueryJournalEvents(query)
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, journal.ErrInvalidQuery) {
			status = http.StatusBadRequest
		}
		writeError(w, r, status, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}
