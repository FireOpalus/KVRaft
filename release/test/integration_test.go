package test

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"release/internal/kv"
	"release/internal/shardctrler"
	"release/pkg/shardcfg"
	"release/pkg/storage"
	"release/pkg/transport"
)

func startServer(me int, ports []string, persister *storage.FilePersister) (*transport.Server, *kv.KVServer) {
	peers := make([]string, len(ports))
	for i, p := range ports {
		peers[i] = "localhost" + p
	}

	clients := make([]*transport.ClientEnd, len(peers))
	for i, addr := range peers {
		clients[i] = transport.MakeClientEnd(addr)
	}

	kvServer := kv.StartKVServer(clients, me, persister, 1000)

	server := transport.NewServer()
	server.Register(kvServer)

	startRaft := kvServer.Raft()
	server.RegisterName("Raft", startRaft)

	go func() {
		err := server.Start(ports[me])
		if err != nil {
		}
	}()

	return server, kvServer
}

func TestKVAndRaft(t *testing.T) {
	fmt.Println("--- Starting TestKVAndRaft ---")

	basePort := 9000
	n := 3
	ports := make([]string, n)
	for i := 0; i < n; i++ {
		ports[i] = ":" + strconv.Itoa(basePort+i)
	}

	servers := make([]*transport.Server, n)
	kvServers := make([]*kv.KVServer, n)

	for i := 0; i < n; i++ {
		dir := fmt.Sprintf("test_raft_data_%d", i)
		os.RemoveAll(dir)
		persister := storage.MakeFilePersister(dir)
		servers[i], kvServers[i] = startServer(i, ports, persister)
	}

	time.Sleep(2 * time.Second)

	peerAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		peerAddrs[i] = "localhost" + ports[i]
	}
	ck := kv.MakeClerk(peerAddrs)

	fmt.Println("Testing Put...")
	key := "testkey"
	val := "testval"
	ck.Put(key, val)

	fmt.Println("Testing Get...")
	got := ck.Get(key)
	if got != val {
		t.Fatalf("Get: expected %v, got %v", val, got)
	}
	fmt.Printf("Get success: %v\n", got)

	leaderCount := 0
	for _, kvs := range kvServers {
		_, isLeader := kvs.Raft().GetState()
		if isLeader {
			leaderCount++
		}
	}
	if leaderCount == 0 {
		t.Error("No leader found")
	} else {
		fmt.Printf("Found %d leader(s)\n", leaderCount)
	}

	for i := 0; i < n; i++ {
		servers[i].Close()
		kvServers[i].Kill()
		kvServers[i].Raft().Kill()
	}

	for i := 0; i < n; i++ {
		dir := fmt.Sprintf("test_raft_data_%d", i)
		os.RemoveAll(dir)
	}
	fmt.Println("--- Passed TestKVAndRaft ---")
}

func TestPersistence(t *testing.T) {
	fmt.Println("--- Starting TestPersistence ---")

	// Create a dummy persister and verify it writes files
	dir := "test_persist_check"
	os.RemoveAll(dir)
	p := storage.MakeFilePersister(dir)

	state := []byte("some state")
	snap := []byte("some snap")
	p.Save(state, snap)

	p2 := storage.MakeFilePersister(dir)
	if p2.RaftStateSize() != len(state) {
		t.Errorf("Expected raft state size %d, got %d", len(state), p2.RaftStateSize())
	}
	if p2.SnapshotSize() != len(snap) {
		t.Errorf("Expected snapshot size %d, got %d", len(snap), p2.SnapshotSize())
	}
	os.RemoveAll(dir)
	fmt.Println("--- Passed TestPersistence Check ---")
}

func TestController(t *testing.T) {
	fmt.Println("--- Starting TestController ---")

	basePort := 9200
	n := 3
	ports := make([]string, n)
	for i := 0; i < n; i++ {
		ports[i] = ":" + strconv.Itoa(basePort+i)
	}

	servers := make([]*transport.Server, n)
	kvServers := make([]*kv.KVServer, n)

	for i := 0; i < n; i++ {
		dir := fmt.Sprintf("test_ctrl_data_%d", i)
		os.RemoveAll(dir)
		persister := storage.MakeFilePersister(dir)
		servers[i], kvServers[i] = startServer(i, ports, persister)
	}
	time.Sleep(2 * time.Second)

	peerAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		peerAddrs[i] = "localhost" + ports[i]
	}

	ctrler := shardctrler.MakeShardCtrler(peerAddrs)
	ctrler.Init()

	cfg := ctrler.Query(-1)
	if cfg.Num != 1 {
		t.Errorf("Expected config Num 1, got %d", cfg.Num)
	}

	groups := map[shardcfg.Tgid][]string{
		100: []string{"s1", "s2"},
		101: []string{"s3", "s4"},
	}
	ctrler.Join(groups)

	cfg = ctrler.Query(-1)
	if cfg.Num != 2 {
		t.Errorf("Expected config Num 2, got %d", cfg.Num)
	}
	if len(cfg.Groups) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(cfg.Groups))
	}

	// Check rebalance
	shardsAssigned := 0
	for _, gid := range cfg.Shards {
		if gid != 0 {
			shardsAssigned++
		}
	}
	if shardsAssigned != shardcfg.NShards {
		t.Errorf("Expected all %d shards assigned, got %d", shardcfg.NShards, shardsAssigned)
	}

	ctrler.Leave([]shardcfg.Tgid{100})
	cfg = ctrler.Query(-1)
	if cfg.Num != 3 {
		t.Errorf("Expected config Num 3, got %d", cfg.Num)
	}

	for i := 0; i < n; i++ {
		servers[i].Close()
		kvServers[i].Kill()
		kvServers[i].Raft().Kill()
	}

	for i := 0; i < n; i++ {
		os.RemoveAll(fmt.Sprintf("test_ctrl_data_%d", i))
	}
	fmt.Println("--- Passed TestController ---")
}
