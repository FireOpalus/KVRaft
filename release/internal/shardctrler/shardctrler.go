package shardctrler

import (
	"encoding/json"

	"release/internal/kv"
	"release/pkg/shardcfg"
)

var shardConfigName string = "shardconfig_curr"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	ck *kv.Clerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(servers []string) *ShardCtrler {
	ck := kv.MakeClerk(servers)
	return &ShardCtrler{ck: ck}
}

// Init ensures the config exists.
func (sck *ShardCtrler) Init() {
	// Try to get current config
	v := sck.ck.Get(shardConfigName)
	if v == "" {
		// Initialize with empty config
		cfg := shardcfg.MakeShardConfig()
		cfg.Num = shardcfg.NumFirst
		bytes, _ := json.Marshal(cfg)
		sck.ck.Put(shardConfigName, string(bytes))
	}
}

func (sck *ShardCtrler) Query(num shardcfg.Tnum) *shardcfg.ShardConfig {
	v := sck.ck.Get(shardConfigName)
	if v == "" {
		return shardcfg.MakeShardConfig()
	}
	var cfg shardcfg.ShardConfig
	json.Unmarshal([]byte(v), &cfg)

	// If num is -1 or > current, return current.
	// Note: simplified logic as we only store one config here for now.
	// A real controller stores history.
	return &cfg
}

func (sck *ShardCtrler) Join(groups map[shardcfg.Tgid][]string) {
	// Lock? ...
	v := sck.ck.Get(shardConfigName)
	var cfg shardcfg.ShardConfig
	if v != "" {
		json.Unmarshal([]byte(v), &cfg)
	} else {
		cfg = *shardcfg.MakeShardConfig()
		cfg.Num = shardcfg.NumFirst
	}

	newCfg := cfg.Clone()
	newCfg.Num++

	for gid, servers := range groups {
		newCfg.Groups[gid] = servers
	}

	newCfg.Rebalance()

	bytes, _ := json.Marshal(newCfg)
	sck.ck.Put(shardConfigName, string(bytes))
}

func (sck *ShardCtrler) Leave(gids []shardcfg.Tgid) {
	v := sck.ck.Get(shardConfigName)
	var cfg shardcfg.ShardConfig
	json.Unmarshal([]byte(v), &cfg)

	newCfg := cfg.Clone()
	newCfg.Num++

	for _, gid := range gids {
		delete(newCfg.Groups, gid)
	}

	newCfg.Rebalance()

	bytes, _ := json.Marshal(newCfg)
	sck.ck.Put(shardConfigName, string(bytes))
}

func (sck *ShardCtrler) Move(shard shardcfg.Tshid, gid shardcfg.Tgid) {
	v := sck.ck.Get(shardConfigName)
	var cfg shardcfg.ShardConfig
	json.Unmarshal([]byte(v), &cfg)

	newCfg := cfg.Clone()
	newCfg.Num++

	newCfg.Shards[shard] = gid

	bytes, _ := json.Marshal(newCfg)
	sck.ck.Put(shardConfigName, string(bytes))
}
