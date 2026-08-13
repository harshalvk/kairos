// Package shard provides consistent hashing to distribute job types
// across multiple redis shards, so no single redis instance is a
// throughput celling for the whole queue
package shard

import "hash/fnv"

// Router maps to job type to one of N shard indices, deterministically
// -- the same job type always routes to the same shard, which matters
// because per-job-type state (rate limits, circuit breaker) is
// in-memory and per-process, not distributed
type Router struct {
	shardCount int
}

// NewRouter creates a Router across shardCount shards. shardCount must
// be >= 1; a Router with shardCount == 1 is equivalent to no sharding
func NewRouter(shardCount int) *Router {
	if shardCount < 1 {
		shardCount = 1
	}
	return &Router{shardCount: shardCount}
}

// ShardFor returns the shard index (0-based) for jobType
func (r *Router) ShardFor(jobType string) int {
	if r.shardCount == 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(jobType))              // hash.Hash.Write never returns an error
	return int(h.Sum32() % uint32(r.shardCount)) // #nosec G115 -- shardCount is validated to fit within uint32
}

// ShardCount returns the total number of shards
func (r *Router) ShardCount() int {
	return r.shardCount
}
