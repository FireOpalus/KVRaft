package main

import (
	"flag"
	"log"
	"strings"

	"release/internal/kv"
	"release/pkg/storage"
	"release/pkg/transport"
)

func main() {
	var port string
	var me int
	var peersStr string
	flag.StringVar(&port, "port", ":8000", "port to listen on (e.g. :8000)")
	flag.IntVar(&me, "me", -1, "my index in peers")
	flag.StringVar(&peersStr, "peers", "localhost:8000,localhost:8001,localhost:8002", "comma separated peers addresses")
	flag.Parse()

	if me == -1 {
		log.Fatal("me is required")
	}

	peers := strings.Split(peersStr, ",")
	clients := make([]*transport.ClientEnd, len(peers))
	for i, addr := range peers {
		clients[i] = transport.MakeClientEnd(addr)
	}

	persister := storage.MakeFilePersister("raft_data_" + strings.TrimPrefix(port, ":"))

	kvServer := kv.StartKVServer(clients, me, persister, 1000)

	server := transport.NewServer()

	server.Register(kvServer)

	startRaft := kvServer.Raft()
	if startRaft == nil {
		log.Fatal("Raft instance is nil")
	}
	server.RegisterName("Raft", startRaft)

	log.Printf("Starting server %d on %s", me, port)
	if err := server.Start(port); err != nil {
	}

	select {}
}
