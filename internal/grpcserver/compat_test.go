package grpcserver_test

import (
	"context"
	"log/slog"
	"net"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/harshalvk/kairos/internal/grpcserver"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/tenant"
	"github.com/harshalvk/kairos/pkg/kairosclient"
)

const bufSize = 1024 * 1024

// startTestServer spins up a real gRPC server over an in-memory
// connection (bufconn) — no real network/port needed, but the actual
// gRPC wire protocol and generated code are exercised exactly as they
// would be in production. Returns a dialer func suitable for
// grpc.WithContextDialer, and a cleanup func.
func startTestServer(t *testing.T) (dial func(context.Context, string) (net.Conn, error), cleanup func()) {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	opts, err := redis.ParseURL(connStr)
	require.NoError(t, err)

	rdb := redis.NewClient(opts)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	grpcserverInstance := grpcserver.New(q, slog.Default())
	// Register against the generated service registration — imported
	// via internal/grpcserver's own package so this test doesn't need
	// to import kairospb directly just to register.
	grpcserver.Register(srv, grpcserverInstance)

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Errorf("gRPC server failed: %v", err)
		}
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	return dialer, func() {
		srv.Stop()
		require.NoError(t, rdb.Close())
	}
}

// TestCompat_SDKMatchesServerContract exercises kairosclient's real
// public methods against a real (bufconn-backed) grpcserver instance —
// this is the test that fails if the SDK's request/response handling
// ever silently drifts from what the server actually implements, e.g.
// a proto field rename or a changed default that only one side still
// assumes. Neither grpcserver's own isolated tests nor a hypothetical
// kairosclient-only test (with no real server) would catch drift
// between the two — only exercising them together does.
func TestCompat_SDKMatchesServerContract(t *testing.T) {
	dial, cleanup := startTestServer(t)
	defer cleanup()

	client, err := kairosclient.ConnectWithOptions("passthrough:///bufnet",
		grpc.WithContextDialer(dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	ctx := context.Background()

	jobID, enqueued, err := client.Enqueue(ctx, "compat_test", []byte(`{}`), kairosclient.EnqueueOptions{})
	require.NoError(t, err)
	assert.True(t, enqueued)
	assert.NotEmpty(t, jobID)

	depth, err := client.QueueDepth(ctx)
	require.NoError(t, err)
	assert.Contains(t, depth, "default")
}
