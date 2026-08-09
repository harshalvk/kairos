// Package api expoes an http interface for inspecting and mananging the
// kairos(job-queue): dead-letter operations, queue depth, and job status lookup
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/store"
	"github.com/harshalvk/kairos/internal/tenant"
)

// statusRecorder wraps http.ResponseWriter to capture the status code
// actually written, for structured request logging. Implemented directly
// rather than relying on a third-party wrapper, since correctly capturing
// WriteHeader is simple enough to own outright.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

type enqueueRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
	Priority    job.Priority    `json:"priority,omitempty"`
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

// Write ensures a status is recorded even if the handler never calls
// WriteHeader explicitly (net/http defaults to 200 in that case).
func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

// Server wraps the http handlers for the admin api
type Server struct {
	queue  *queue.Queue
	store  *store.Store
	logger *slog.Logger
}

// New creates an admin api server
func New(q *queue.Queue, s *store.Store, logger *slog.Logger) *Server {
	return &Server{queue: q, store: s, logger: logger}
}

// Routes returns an http.Handler with all admin api routes registered
func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(s.loggingMiddleware)
	r.Use(s.tenantMiddleware)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(10 * time.Second))

	r.Get("/healthz", s.handleHealthz)
	r.Get("/queue/depth", s.handleQueueDepth)
	r.Get("/jobs/{id}/result", s.handleGetResult)

	r.Route("/jobs", func(r chi.Router) {
		r.Post("/", s.handleEnqueue)
		r.Route("/dead-letter", func(r chi.Router) {
			r.Get("/", s.handleListDeadLetter)
			r.Delete("/", s.handlePurgeDeadLetter)
			r.Post("/{id}/requeue", s.handleRequeueDeadLetter)
		})
	})

	return r
}

// loggingMiddleware logs each request with method, path status, and
// duration using the server's structured logger
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}

		next.ServeHTTP(rec, r)
		s.logger.Info("admin api request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)),
		)
	})

}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Error("failed to encode json response", slog.Any("error", err))
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{"error": message})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})

}

func (s *Server) handleQueueDepth(w http.ResponseWriter, r *http.Request) {
	depths := make(map[string]int64)
	for _, p := range []job.Priority{job.PriorityHigh, job.PriorityDefault, job.PriorityLow} {
		depth, err := s.queue.Depth(r.Context(), p)
		if err != nil {
			s.logger.Error("failed to get queue depth", slog.String("priority", string(p)), slog.Any("error", err))
			s.writeError(w, http.StatusInternalServerError, "failed to get queue depth")
			return
		}
		depths[string(p)] = depth
	}
	s.writeJSON(w, http.StatusOK, depths)
}

func (s *Server) handleListDeadLetter(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.queue.ListDeadLetter(r.Context(), 100)
	if err != nil {
		s.logger.Error("failed to list dead-letter jobs", slog.Any("error", err))
		s.writeError(w, http.StatusInternalServerError, "failed to list dead-letter jobs")
		return
	}
	s.writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) handleRequeueDeadLetter(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		s.writeError(w, http.StatusBadRequest, "job id is required")
		return
	}

	if err := s.queue.RequeueDeadLetter(r.Context(), id); err != nil {
		s.logger.Error("failed to requeue dead-letter job", slog.String("job_id", id), slog.Any("error", err))
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "requeued", "job_id": id})
}

func (s *Server) handlePurgeDeadLetter(w http.ResponseWriter, r *http.Request) {
	if err := s.queue.PurgeDeadLetter(r.Context()); err != nil {
		s.logger.Error("failed to purge dead-letter queue", slog.Any("error", err))
		s.writeError(w, http.StatusInternalServerError, "failed to purge dead-letter queue")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "purged"})
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid reqeust body")
		return
	}

	if req.Type == "" {
		s.writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 3
	}
	if req.Priority == "" {
		req.Priority = job.PriorityDefault
	}

	j := job.NewWithPriority(req.Type, req.Payload, req.MaxAttempts, req.Priority)
	if err := s.queue.Enqueue(r.Context(), j); err != nil {
		s.logger.Error("failed to enqueue job", slog.Any("error", err))
		s.writeError(w, http.StatusInternalServerError, "failed to enqueue job")
		return
	}
	s.writeJSON(w, http.StatusCreated, map[string]string{"job_id": j.ID})
}

func (s *Server) tenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.Header.Get(("X-Tenant-ID"))
		if tenantID == "" {
			tenantID = tenant.DefaultTenant
		}
		if err := tenant.Validate(tenantID); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid X-Tenant-ID header")
			return
		}
		ctx := tenant.WithContext(r.Context(), tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) handleGetResult(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.store == nil {
		s.writeError(w, http.StatusNotImplemented, "result storage not configured")
		return
	}
	result, err := s.store.GetResult(r.Context(), id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "job not found or has no result")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// #nosec G705 -- result is read from a jsonb Postgres column (always
	// valid JSON by construction) and served with an explicit JSON
	// content-type plus nosniff, so it can never be interpreted as HTML
	// by a browser. This is not an XSS vector.
	if _, err := w.Write(result); err != nil {
		s.logger.Error("failed to write result response", slog.String("job_id", id), slog.Any("error", err))
	}
}
