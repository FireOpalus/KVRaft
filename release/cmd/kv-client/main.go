package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"release/internal/kv"
)

func main() {
	var peersStr string
	var op string
	var key string
	var value string

	flag.StringVar(&peersStr, "peers", "localhost:8000,localhost:8001,localhost:8002", "comma separated peers addresses")
	flag.StringVar(&op, "op", "get", "operation: get, put")
	flag.StringVar(&key, "key", "test", "key")
	flag.StringVar(&value, "value", "val", "value for put")
	flag.Parse()

	peers := strings.Split(peersStr, ",")
	ck := kv.MakeClerk(peers)

	switch op {
	case "put":
		ck.Put(key, value)
		fmt.Printf("Put %s = %s\n", key, value)
	case "get":
		v := ck.Get(key)
		fmt.Printf("Get %s = %s\n", key, v)
	default:
		log.Fatalf("Unknown op: %s", op)
	}
}
