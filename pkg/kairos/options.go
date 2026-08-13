package kairos

import (
	"log/slog"
	"time"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
)

type config struct {
	redisAddr        string
	postgresDSN      string
	tenantID         string
	nodeID           string
	concurrency      int
	circuitThreshold int
	circuitCooldown  time.Duration
	logger           *slog.Logger
	backpressure     *queue.BackpressureConfig
}

func defaultConfig() config {
	return config{
		redisAddr:        "localhost:6379",
		tenantID:         "default",
		nodeID:           "kairos-client",
		concurrency:      5,
		circuitThreshold: 5,
		circuitCooldown:  30 * time.Second,
	}
}

// Option configures a Kairos client.
type Option func(*config)

// WithRedisAddr sets the Redis address to connect to (default "localhost:6379").
func WithRedisAddr(addr string) Option { return func(c *config) { c.redisAddr = addr } }

// WithPostgresDSN sets the Postgres connection string. If unset, job
// history is not persisted.
func WithPostgresDSN(dsn string) Option { return func(c *config) { c.postgresDSN = dsn } }

// WithTenant scopes this client to tenantID (default "default").
func WithTenant(id string) Option { return func(c *config) { c.tenantID = id } }

// WithNodeID sets the identifier used to attribute logs to this process.
func WithNodeID(id string) Option { return func(c *config) { c.nodeID = id } }

// WithConcurrency sets the number of concurrent worker goroutines (default 5).
func WithConcurrency(n int) Option { return func(c *config) { c.concurrency = n } }

// WithLogger sets a custom structured logger, used instead of the default.
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// WithCircuitBreaker configures the failure threshold and cooldown for
// the per-job-type circuit breaker.
func WithCircuitBreaker(threshold int, cooldown time.Duration) Option {
	return func(c *config) { c.circuitThreshold = threshold; c.circuitCooldown = cooldown }
}

// --- job registration options (Handle) ---

type jobOptions struct {
	maxAttempts int
	priority    job.Priority
	rateLimit   float64
	rateBurst   int
}

// JobOption configures how a registered handler behaves.
type JobOption func(*jobOptions)

// MaxAttempts sets the maximum retry attempts for jobs handled by this handler.
func MaxAttempts(n int) JobOption { return func(o *jobOptions) { o.maxAttempts = n } }

// RateLimit caps this job type to perSecond sustained throughput, with
// burst allowing short spikes above that rate.
func RateLimit(perSecond float64, burst int) JobOption {
	return func(o *jobOptions) { o.rateLimit = perSecond; o.rateBurst = burst }
}

// --- enqueue options (Enqueue) ---

type enqueueOptions struct {
	maxAttempts    int
	priority       job.Priority
	dependsOn      []string
	idempotencyKey string
	webhook        *job.WebhookConfig
}

// EnqueueOption configures a single Enqueue call.
type EnqueueOption func(*enqueueOptions)

// Attempts sets the maximum retry attempts for this specific enqueue call.
func Attempts(n int) EnqueueOption { return func(o *enqueueOptions) { o.maxAttempts = n } }

// Priority sets the queue priority for this job (kairos.High/Default/Low).
func Priority(p job.Priority) EnqueueOption { return func(o *enqueueOptions) { o.priority = p } }

// DependsOn makes this job wait until every job in jobIDs has completed successfully.
func DependsOn(jobIDs ...string) EnqueueOption {
	return func(o *enqueueOptions) { o.dependsOn = jobIDs }
}

// IdempotencyKey deduplicates enqueue calls sharing the same job type and key.
func IdempotencyKey(key string) EnqueueOption {
	return func(o *enqueueOptions) { o.idempotencyKey = key }
}

// Re-export priority constants so callers never need to import
// internal/job directly.
const (
	High    = job.PriorityHigh
	Default = job.PriorityDefault
	Low     = job.PriorityLow
)

// WithWebhook configures a callback URL fired on the given lifecycle
// events (subset of "completed", "failed", "dead_letter").
func WithWebhook(url string, events ...string) EnqueueOption {
	return func(o *enqueueOptions) { o.webhook = &job.WebhookConfig{URL: url, Events: events} }
}
