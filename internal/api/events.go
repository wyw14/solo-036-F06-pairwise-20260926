package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/wyw14/solo-036/internal/journal"
	"github.com/wyw14/solo-036/internal/service"
)

// Events serves GET /api/events. Optional query parameters: run_id and
// kind exact-match filters, from/to RFC3339 time bounds, the after resume
// cursor returned as next by the previous page, and the limit page size.
func (h *Handler) Events(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	query := journal.EventQuery{
		RunID: values.Get("run_id"),
		Kind:  values.Get("kind"),
	}
	if raw := values.Get("from"); raw != "" {
		from, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.New("from must use RFC3339 format"))
			return
		}
		query.From = from.UTC()
	}
	if raw := values.Get("to"); raw != "" {
		to, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.New("to must use RFC3339 format"))
			return
		}
		query.To = to.UTC()
	}
	if raw := values.Get("after"); raw != "" {
		after, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.New("after must be an integer sequence"))
			return
		}
		query.After = after
	}
	if raw := values.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 {
			writeError(w, r, http.StatusBadRequest, errors.New("limit must be a positive integer"))
			return
		}
		query.Limit = limit
	}
	page, err := h.runtime.QueryEvents(query)
	if err != nil {
		switch {
		case errors.Is(err, journal.ErrInvalidQuery):
			writeError(w, r, http.StatusBadRequest, err)
		case errors.Is(err, service.ErrPersistenceNotConfigured):
			writeError(w, r, http.StatusConflict, err)
		default:
			writeError(w, r, http.StatusInternalServerError, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, page)
}
