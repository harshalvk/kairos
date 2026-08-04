package api_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	redislib "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/harshalvk/kairos/internal/api"
	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/tenant"
)

func setupServer(t *testing.T) (*httptest.Server, *queue.Queue) {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	opts, err := redislib.ParseURL(connStr)
	require.NoError(t, err)
	rdb := redislib.NewClient(opts)
	registry := tenant.NewRegistry(rdb)
	q := queue.New(rdb, registry)
	logger := slog.Default()
	srv := api.New(q, nil, logger)

	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)
	return ts, q
}

// doRequest builds and executes an HTTP request with a proper context
// (satisfies the noctx linter) and returns the response, with a
// t.Cleanup registered to close the body (satisfies errcheck without
// scattering `defer func() { _ = resp.Body.Close() }()` everywhere).
//
// Uses a dedicated client with keep-alives disabled rather than
// http.DefaultClient: DefaultClient is a package-level global shared
// across every test in this file, and its pooled connections are keyed
// by host:port — since each test spins up its own short-lived
// httptest.Server, a rapidly reused local port can otherwise hand back
// a stale pooled connection from a previous test's (now-closed) server.
func doRequest(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	ctx := context.Background()

	reqBody := bytes.NewReader(body)

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	require.NoError(t, err)

	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	resp, err := client.Do(req)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	return resp
}

func TestHealthz(t *testing.T) {
	ts, _ := setupServer(t)

	resp := doRequest(t, http.MethodGet, ts.URL+"/healthz", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestQueueDepth_EmptyQueue(t *testing.T) {
	ts, _ := setupServer(t)

	resp := doRequest(t, http.MethodGet, ts.URL+"/queue/depth", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRequeueDeadLetter_NotFound(t *testing.T) {
	ts, _ := setupServer(t)

	resp := doRequest(t, http.MethodPost, ts.URL+"/jobs/dead-letter/nonexistent-id/requeue", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestRequeueDeadLetter_Success(t *testing.T) {
	ts, q := setupServer(t)
	ctx := context.Background()

	j := job.New("send_email", []byte(`{"to":"test@example.com"}`), 3)
	j.Attempts = 3
	require.NoError(t, q.MoveToDeadLetter(ctx, j))

	resp := doRequest(t, http.MethodPost, ts.URL+"/jobs/dead-letter/"+j.ID+"/requeue", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestListDeadLetter(t *testing.T) {
	ts, q := setupServer(t)
	ctx := context.Background()

	j := job.New("send_email", []byte(`{"to":"test@example.com"}`), 3)
	require.NoError(t, q.MoveToDeadLetter(ctx, j))

	resp := doRequest(t, http.MethodGet, ts.URL+"/jobs/dead-letter", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestPurgeDeadLetter(t *testing.T) {
	ts, _ := setupServer(t)

	resp := doRequest(t, http.MethodDelete, ts.URL+"/jobs/dead-letter", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
