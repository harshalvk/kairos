package shard_test

import (
	"testing"

	"github.com/harshalvk/kairos/internal/shard"
	"github.com/stretchr/testify/assert"
)

func TestShardFor_IsDeterministic(t *testing.T) {
	r := shard.NewRouter(4)
	first := r.ShardFor("send_email")
	for i := 0; i < 100; i++ {
		assert.Equal(t, first, r.ShardFor("send_email"))
	}
}

func TestShardFor_DistributesAcrossShards(t *testing.T) {
	r := shard.NewRouter(4)
	seen := make(map[int]bool)
	types := []string{"send_email", "resize_image", "charge_card", "sync_inventory", "generate_report", "notify_user"}
	for _, t := range types {
		seen[r.ShardFor(t)] = true
	}
	assert.Greater(t, len(seen), 1, "expected job types to spread across more than one shard")
}

func TestShardFor_SingleShardAlwaysReturnsZero(t *testing.T) {
	r := shard.NewRouter(1)
	assert.Equal(t, 0, r.ShardFor("anything"))
}
