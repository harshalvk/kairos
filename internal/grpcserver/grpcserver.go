// Package grpcserver implements the kairos gRPC service, wrapping the
// same queue.Queue operations the admin http api and cli use
package grpcserver

import (
	"context"
	"log/slog"
	"time"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/harshalvk/kairos/pkg/kairospb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server implements kairospb.KairosServiceServer
type Server struct {
	kairospb.UnimplementedKairosServiceServer
	queue  *queue.Queue
	logger *slog.Logger
}

// New creates a gRPC server backed by q
func New(q *queue.Queue, logger *slog.Logger) *Server {
	return &Server{queue: q, logger: logger}
}

func priorityFromProto(p kairospb.Priority) job.Priority {
	switch p {
	case kairospb.Priority_PRIORITY_HIGH:
		return job.PriorityHigh
	case kairospb.Priority_PRIORITY_LOW:
		return job.PriorityLow
	default:
		return job.PriorityDefault
	}
}

// Enqueue implements kairospb.KairosServiceServer.
func (s *Server) Enqueue(ctx context.Context, req *kairospb.EnqueueRequest) (*kairospb.EnqueueResponse, error) {
	if req.GetType() == "" {
		return nil, status.Error(codes.InvalidArgument, "type is required")
	}
	maxAttempts := int(req.GetMaxAttempts())
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	j := job.NewWithPriority(req.GetType(), req.GetPayload(), maxAttempts, priorityFromProto(req.GetPriority()))

	if req.GetIdempotencyKey() != "" {
		j.IdempotencyKey = req.GetIdempotencyKey()
		enqueued, err := s.queue.EnqueueIdempotent(ctx, j, 24*time.Hour)
		if err != nil {
			s.logger.Error("grpc enqueue failed", slog.Any("error", err))
			return nil, status.Error(codes.Internal, "failed to enqueue job")
		}

		return &kairospb.EnqueueResponse{JobId: j.ID, Enqueued: enqueued}, nil
	}

	if err := s.queue.Enqueue(ctx, j); err != nil {
		s.logger.Error("grpc enqueue failed", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to enqueue job")
	}

	return &kairospb.EnqueueResponse{JobId: j.ID, Enqueued: true}, nil
}

// GetQueueDepth implements kairospb.KairosServiceServer.
func (s *Server) GetQueueDepth(ctx context.Context, _ *kairospb.GetQueueDepthRequest) (*kairospb.GetQueueDepthResponse, error) {
	depths := make(map[string]int64)
	for _, p := range []job.Priority{job.PriorityHigh, job.PriorityDefault, job.PriorityLow} {
		depth, err := s.queue.Depth(ctx, p)
		if err != nil {
			s.logger.Error("grpc get queue depth failed", slog.Any("error", err))
			return nil, status.Error(codes.Internal, "failed to get queue depth")
		}
		depths[string(p)] = depth
	}
	return &kairospb.GetQueueDepthResponse{DepthByPriority: depths}, nil
}

// ListDeadLetter implements kairospb.KairosServiceServer.
func (s *Server) ListDeadLetter(ctx context.Context, req *kairospb.ListDeadLetterRequest) (*kairospb.ListDeadLetterResponse, error) {
	limit := req.GetLimit()
	if limit <= 0 {
		limit = 100
	}
	jobs, err := s.queue.ListDeadLetter(ctx, limit)
	if err != nil {
		s.logger.Error("grpc list dead letter failed", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to list dead-letter jobs")
	}

	pbJobs := make([]*kairospb.Job, 0, len(jobs))
	for _, j := range jobs {
		pbJobs = append(pbJobs, &kairospb.Job{
			Id:          j.ID,
			Type:        j.Type,
			Payload:     j.Payload,
			Status:      string(j.Status),
			Attempts:    job.ToInt32(j.Attempts),
			MaxAttempts: job.ToInt32(j.MaxAttempts),
			LastError:   j.LastError,
			CreatedAt:   timestamppb.New(j.CreatedAt),
		})
	}
	return &kairospb.ListDeadLetterResponse{Jobs: pbJobs}, nil
}

// RequeueDeadLetter implements kairospb.KairosServiceServer.
func (s *Server) RequeueDeadLetter(ctx context.Context, req *kairospb.RequeueDeadLetterRequest) (*kairospb.RequeueDeadLetterResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if err := s.queue.RequeueDeadLetter(ctx, req.GetJobId()); err != nil {
		return nil, status.Error(codes.NotFound, "job not found in dead-letter queue")
	}
	return &kairospb.RequeueDeadLetterResponse{Requeued: true}, nil
}

// TenantInterceptor reads a "tenant-id" gRPC metadata key and attaches
// it to the request context, mirroring the admin api's X-Tenant-ID
// header handling
func TenantInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	tenantID := tenant.DefaultTenant
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("tenant-id"); len(vals) > 0 && vals[0] != "" {
			tenantID = vals[0]
		}
	}
	if err := tenant.Validate(tenantID); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid tenant-id metadata")
	}
	return handler(tenant.WithContext(ctx, tenantID), req)
}
