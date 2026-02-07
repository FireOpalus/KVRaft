package test

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"release/internal/kv"
	rpc "release/internal/kvrpc"
	"release/internal/mr"
	"release/internal/shardctrler"
	"release/internal/shardkv"
	shardrpc "release/internal/shardkvrpc"
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

// --- Lab 1: MapReduce Tests ---

func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}

func TestMapReduce(t *testing.T) {
	fmt.Println("--- Starting TestMapReduce ---")

	// Prepare input
	files := []string{"pg-being_ernest.txt"}
	content := "foo bar foo"
	os.WriteFile("pg-being_ernest.txt", []byte(content), 0644)
	defer os.Remove("pg-being_ernest.txt")

	mr.CoordinatorPort = "2234"

	// Start Coordinator
	c := mr.MakeCoordinator(files, 1)

	// Start Worker
	go mr.Worker(Map, Reduce)

	// Wait for completion
	done := false
	for i := 0; i < 10; i++ {
		if c.Done() {
			done = true
			break
		}
		time.Sleep(time.Second)
	}

	if !done {
		t.Fatal("MapReduce failed to finish in time")
	}

	// helper function to verify output
	// read mr-out-0
	out, err := os.ReadFile("mr-out-0")
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	defer os.Remove("mr-out-0")

	sout := string(out)
	if !strings.Contains(sout, "foo 2") || !strings.Contains(sout, "bar 1") {
		t.Errorf("Unexpected output: %s", sout)
	}

	fmt.Println("--- Passed TestMapReduce ---")
}

func TestMapReduceCrash(t *testing.T) {
	fmt.Println("--- Starting TestMapReduceCrash ---")

	// Prepare input
	files := []string{"pg-crash.txt"}
	content := "foo bar foo"
	os.WriteFile("pg-crash.txt", []byte(content), 0644)
	defer os.Remove("pg-crash.txt")

	mr.CoordinatorPort = "2235" // Different port

	// Start Coordinator with 1 reduce task
	c := mr.MakeCoordinator(files, 1)

	// Start Slow Worker (will crash/timeout)
	go func() {
		slowMap := func(filename string, contents string) []mr.KeyValue {
			time.Sleep(12 * time.Second) // Longer than 10s timeout
			return Map(filename, contents)
		}
		mr.Worker(slowMap, Reduce)
	}()

	// Start Fast Worker after a delay (to pick up the timed-out task)
	go func() {
		time.Sleep(2 * time.Second)
		mr.Worker(Map, Reduce)
	}()

	// Wait for completion
	done := false
	for i := 0; i < 20; i++ { // Wait longer for timeout + retry
		if c.Done() {
			done = true
			break
		}
		time.Sleep(time.Second)
	}

	if !done {
		t.Fatal("MapReduce Crash test failed to finish in time")
	}

	// Verify output
	out, err := os.ReadFile("mr-out-0")
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	defer os.Remove("mr-out-0")

	sout := string(out)
	if !strings.Contains(sout, "foo 2") || !strings.Contains(sout, "bar 1") {
		t.Errorf("Unexpected output: %s", sout)
	}

	fmt.Println("--- Passed TestMapReduceCrash ---")
}

func TestRaftFailover(t *testing.T) {
	fmt.Println("--- Starting TestRaftFailover ---")

	basePort := 9100
	n := 3
	ports := make([]string, n)
	for i := 0; i < n; i++ {
		ports[i] = ":" + strconv.Itoa(basePort+i)
	}

	servers := make([]*transport.Server, n)
	kvServers := make([]*kv.KVServer, n)

	// Start Cluster
	for i := 0; i < n; i++ {
		dir := fmt.Sprintf("test_failover_data_%d", i)
		os.RemoveAll(dir)
		persister := storage.MakeFilePersister(dir)
		servers[i], kvServers[i] = startServer(i, ports, persister)
	}
	defer func() {
		for i := 0; i < n; i++ {
			if servers[i] != nil {
				servers[i].Close()
			}
			if kvServers[i] != nil {
				kvServers[i].Kill()
				kvServers[i].Raft().Kill()
			}
			dir := fmt.Sprintf("test_failover_data_%d", i)
			os.RemoveAll(dir)
		}
	}()

	time.Sleep(2 * time.Second)

	peerAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		peerAddrs[i] = "localhost" + ports[i]
	}
	ck := kv.MakeClerk(peerAddrs)

	// Initial Put
	ck.Put("a", "1")
	if val := ck.Get("a"); val != "1" {
		t.Fatalf("Msg: expected 1, got %v", val)
	}

	// Find Leader
	leaderId := -1
	for i := 0; i < n; i++ {
		_, isLeader := kvServers[i].Raft().GetState()
		if isLeader {
			leaderId = i
			break
		}
	}

	if leaderId == -1 {
		t.Fatal("No leader found")
	}
	fmt.Printf("Killing leader %d\n", leaderId)

	// Kill Leader
	servers[leaderId].Close()
	kvServers[leaderId].Kill()
	kvServers[leaderId].Raft().Kill()
	servers[leaderId] = nil // Mark for cleanup skip

	// Give time for election
	time.Sleep(2 * time.Second)

	// Put new data
	fmt.Println("Putting data to new leader...")
	ck.Put("b", "2")
	if val := ck.Get("b"); val != "2" {
		t.Fatalf("Get b: expected 2, got %v", val)
	}
	// Check old data availability
	if val := ck.Get("a"); val != "1" {
		t.Fatalf("Get a: expected 1, got %v", val)
	}

	fmt.Println("--- Passed TestRaftFailover ---")
}

// --- Lab 5: ShardKV Tests ---

func startShardKVServer(gid shardcfg.Tgid, me int, ports []string, persister *storage.FilePersister) (*transport.Server, *shardkv.KVServer) {
	peers := make([]string, len(ports))
	for i, p := range ports {
		peers[i] = "localhost" + p
	}

	clients := make([]*transport.ClientEnd, len(peers))
	for i, addr := range peers {
		clients[i] = transport.MakeClientEnd(addr)
	}

	kvServer := shardkv.StartServerShardGrp(clients, gid, me, persister, 1000)

	server := transport.NewServer()
	server.Register(kvServer)

	startRaft := kvServer.Raft()
	server.RegisterName("Raft", startRaft)

	go func() {
		err := server.Start(ports[me])
		if err != nil {
			// log?
		}
	}()

	return server, kvServer
}

// --- Lab 5: ShardKV Tests ---

func TestShardKVStatic(t *testing.T) {
	fmt.Println("--- Starting TestShardKVStatic ---")

	gid1 := shardcfg.Gid1
	n := 3
	ports, servers, kvServers := setupShardKVGroup(t, gid1, 9300, n)
	defer cleanupShardKVGroup(servers, kvServers, 9300, n)

	time.Sleep(2 * time.Second)

	serverAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		serverAddrs[i] = "localhost" + ports[i]
	}
	ck := shardkv.MakeClerk(serverAddrs)

	// Basic Put/Get on Gid1 (Default config assigns all to Gid1)
	fmt.Println("ShardKV Put...")
	// Try multiple keys to ensure they all map to standard Gid1
	keys := []string{"key1", "key2", "key3", "a", "z"}
	for _, k := range keys {
		err := ck.Put(k, "val-"+k, 0, 0)
		if err != rpc.OK {
			t.Fatalf("Put %v failed: %v", k, err)
		}
	}

	fmt.Println("ShardKV Get...")
	for _, k := range keys {
		val, _, err := ck.Get(k, 0)
		if err != rpc.OK {
			t.Fatalf("Get %v failed: %v", k, err)
		}
		if val != "val-"+k {
			t.Errorf("Get %v expected val-%v, got %v", k, k, val)
		}
	}

	fmt.Println("--- Passed TestShardKVStatic ---")
}

func TestShardCtrlerBasic(t *testing.T) {
	fmt.Println("--- Starting TestShardCtrlerBasic ---")

	// 1. Start a KV Request Service (for ShardCtrler to store data)
	ctrlerPorts, ctrlerServers, ctrlerKV := setupKVCluster(t, 9400, 3)
	defer cleanupKVCluster(ctrlerServers, ctrlerKV, 9400, 3)

	time.Sleep(1 * time.Second)

	ctrlerAddrs := make([]string, 3)
	for i := 0; i < 3; i++ {
		ctrlerAddrs[i] = "localhost" + ctrlerPorts[i]
	}

	// 2. Start ShardCtrler
	sc := shardctrler.MakeShardCtrler(ctrlerAddrs)
	sc.Init()

	// 3. Query Initial Config (Num=1, Shards all Gid1? No, ShardCtrler logic Init makes empty or custom?)
	// release/internal/shardctrler/shardctrler.go says Init makes empty config.
	cfg := sc.Query(-1)
	if cfg.Num != 1 {
		t.Fatalf("Expected Config Num 1, got %d", cfg.Num)
	}

	// 4. Join Gid 100
	gid100 := shardcfg.Tgid(100)
	sc.Join(map[shardcfg.Tgid][]string{gid100: {"s1", "s2"}})

	cfg = sc.Query(-1)
	if cfg.Num != 2 {
		t.Fatalf("Expected Config Num 2 after Join, got %d", cfg.Num)
	}
	if _, ok := cfg.Groups[gid100]; !ok {
		t.Fatal("Gid 100 not in config after Join")
	}
	// Check distribution: Should have some shards
	hasShards := false
	for _, g := range cfg.Shards {
		if g == gid100 {
			hasShards = true
			break
		}
	}
	if !hasShards {
		t.Fatal("Gid 100 assigned no shards after Join")
	}

	fmt.Println("--- Passed TestShardCtrlerBasic ---")
}

func TestShardKVReconfiguration(t *testing.T) {
	fmt.Println("--- Starting TestShardKVReconfiguration ---")

	// 1. Start Gid 1 (Default owner)
	ports1, srv1, kvs1 := setupShardKVGroup(t, shardcfg.Gid1, 9500, 3)
	defer cleanupShardKVGroup(srv1, kvs1, 9500, 3)

	// 2. Start Gid 2 (New owner check)
	gid2 := shardcfg.Tgid(102)
	ports2, srv2, kvs2 := setupShardKVGroup(t, gid2, 9510, 3)
	defer cleanupShardKVGroup(srv2, kvs2, 9510, 3)

	time.Sleep(2 * time.Second)

	// Clients
	addrs1 := make([]string, 3)
	for i := 0; i < 3; i++ {
		addrs1[i] = "localhost" + ports1[i]
	}
	ck1 := shardkv.MakeClerk(addrs1)

	addrs2 := make([]string, 3)
	for i := 0; i < 3; i++ {
		addrs2[i] = "localhost" + ports2[i]
	}
	ck2 := shardkv.MakeClerk(addrs2)

	// Put data to Gid1 (Shard 0 usually)
	// We need a key that maps to Shard 0.
	// shardcfg.Key2Shard is hash. Simple numbers might map predictably?
	// Let's just write to multiple keys and find one that moves?
	// Or we manually move Shard 0 from Gid1 to Gid2.

	key := "move-me"
	shard := shardcfg.Key2Shard(key)

	fmt.Printf("Putting key '%s' (shard %d) to Gid1\n", key, shard)
	if err := ck1.Put(key, "init-val", 0, 0); err != rpc.OK {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Verify logical movement:
	// We want to move 'shard' from Gid1 to Gid2.

	// Step 1: Freeze Shard on Gid1
	// We need to call FreezeShard RPC.
	// Use the first server of Gid1 (leader hopefully, or try all).
	// Construct FreezeShardArgs

	fmt.Println("Manually Migrating Shard...")

	meta, err := callFreezeShard(t, addrs1, shard, 1) // Num 1 -> 2
	if err != nil {
		t.Fatalf("Freeze failed: %v", err)
	}

	// Step 2: Install Shard on Gid2
	if err := callInstallShard(t, addrs2, meta, 1); err != nil {
		t.Fatalf("Install failed: %v", err)
	}

	// Step 3: Verify Gid2 serves it
	fmt.Println("Verifying Gid2 serves key...")
	val, _, rpcErr := ck2.Get(key, 0)
	if rpcErr != rpc.OK {
		t.Fatalf("Gid2 Get failed: %v", rpcErr)
	}
	if val != "init-val" {
		t.Errorf("Migrated value Mismatch: %v", val)
	}

	fmt.Println("--- Passed TestShardKVReconfiguration ---")
}

// Helpers

func setupKVCluster(t *testing.T, basePort int, n int) ([]string, []*transport.Server, []*kv.KVServer) {
	ports := make([]string, n)
	for i := 0; i < n; i++ {
		ports[i] = ":" + strconv.Itoa(basePort+i)
	}
	servers := make([]*transport.Server, n)
	kvServers := make([]*kv.KVServer, n)
	for i := 0; i < n; i++ {
		dir := fmt.Sprintf("test_kv_data_%d_%d", basePort, i)
		os.RemoveAll(dir)
		persister := storage.MakeFilePersister(dir)
		servers[i], kvServers[i] = startServer(i, ports, persister)
	}
	return ports, servers, kvServers
}

func cleanupKVCluster(servers []*transport.Server, kvServers []*kv.KVServer, basePort int, n int) {
	for i := 0; i < n; i++ {
		if servers[i] != nil {
			servers[i].Close()
		}
		if kvServers[i] != nil {
			kvServers[i].Kill()
			kvServers[i].Raft().Kill()
		}
		os.RemoveAll(fmt.Sprintf("test_kv_data_%d_%d", basePort, i))
	}
}

func setupShardKVGroup(t *testing.T, gid shardcfg.Tgid, basePort int, n int) ([]string, []*transport.Server, []*shardkv.KVServer) {
	ports := make([]string, n)
	for i := 0; i < n; i++ {
		ports[i] = ":" + strconv.Itoa(basePort+i)
	}
	servers := make([]*transport.Server, n)
	kvServers := make([]*shardkv.KVServer, n)
	for i := 0; i < n; i++ {
		dir := fmt.Sprintf("test_shardkv_data_%d_%d", basePort, i)
		os.RemoveAll(dir)
		persister := storage.MakeFilePersister(dir)
		servers[i], kvServers[i] = startShardKVServer(gid, i, ports, persister)
	}
	return ports, servers, kvServers
}

func cleanupShardKVGroup(servers []*transport.Server, kvServers []*shardkv.KVServer, basePort int, n int) {
	for i := 0; i < n; i++ {
		if servers[i] != nil {
			servers[i].Close()
		}
		if kvServers[i] != nil {
			kvServers[i].Kill()
			kvServers[i].Raft().Kill()
		}
		os.RemoveAll(fmt.Sprintf("test_shardkv_data_%d_%d", basePort, i))
	}
}

func callFreezeShard(t *testing.T, servers []string, shard shardcfg.Tshid, num shardcfg.Tnum) (shardrpc.InstallShardArgs, error) {
	args := shardrpc.FreezeShardArgs{Shard: shard, Num: num}
	reply := shardrpc.FreezeShardReply{}

	for _, s := range servers {
		cl := transport.MakeClientEnd(s)
		ok := cl.Call("KVServer.FreezeShard", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				return shardrpc.InstallShardArgs{
					Shard: shard,
					Num:   reply.Num, // Or arg num? Usually freeze returns current num?
					State: reply.State,
				}, nil
			}
			// If wrong leader, continue
		}
	}
	return shardrpc.InstallShardArgs{}, fmt.Errorf("FreezeShard failed on all peers")
}

func callInstallShard(t *testing.T, servers []string, args shardrpc.InstallShardArgs, num shardcfg.Tnum) error {
	reply := shardrpc.InstallShardReply{}
	for _, s := range servers {
		cl := transport.MakeClientEnd(s)
		ok := cl.Call("KVServer.InstallShard", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				return nil
			}
		}
	}
	return fmt.Errorf("InstallShard failed on all peers")
}
