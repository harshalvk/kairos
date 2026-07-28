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
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(10 * time.Second))

	r.Get("/healthz", s.handleHealthz)
	r.Get("/queue/depth", s.handleQueueDepth)

	r.Route("/jobs/dead-letter", func(r chi.Router) {
		r.Get("/", s.handleListDeadLetter)
		r.Delete("/", s.handlePurgeDeadLetter)
		r.Post("/{id}/requeue", s.handleRequeueDeadLetter)
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
