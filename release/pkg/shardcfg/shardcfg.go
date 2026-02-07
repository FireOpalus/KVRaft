package shardcfg

import (
"hash/fnv"
"slices"
)

type Tshid int
type Tgid int
type Tnum int

const (
NShards  = 12 // The number of shards.
NumFirst = Tnum(1)
)

const (
Gid1 = Tgid(1)
)

// which shard is a key in?
func Key2Shard(key string) Tshid {
h := fnv.New32a()
h.Write([]byte(key))
shard := Tshid(Tshid(h.Sum32()) % NShards)
return shard
}

// A configuration -- an assignment of shards to groups.
type ShardConfig struct {
Num    Tnum                     // config number
Shards [NShards]Tgid            // shard -> gid
Groups map[Tgid][]string        // gid -> servers[]
}

func MakeShardConfig() *ShardConfig {
c := &ShardConfig{
Groups: make(map[Tgid][]string),
}
return c
}

func (cfg *ShardConfig) Clone() *ShardConfig {
c := MakeShardConfig()
c.Num = cfg.Num
c.Shards = cfg.Shards
for k, v := range cfg.Groups {
sl := make([]string, len(v))
copy(sl, v)
c.Groups[k] = sl
}
return c
}

func (cfg *ShardConfig) Rebalance() {
if len(cfg.Groups) == 0 {
for i := 0; i < NShards; i++ {
cfg.Shards[i] = 0
}
return
}

gids := make([]Tgid, 0, len(cfg.Groups))
for g := range cfg.Groups {
gids = append(gids, g)
}
slices.Sort(gids)

perGroup := NShards / len(gids)
    remainder := NShards % len(gids)

    shardIdx := 0
    for i, gid := range gids {
        count := perGroup
        if i < int(remainder) {
            count++
        }
        for j := 0; j < count; j++ {
            cfg.Shards[shardIdx] = gid
            shardIdx++
        }
    }
}
