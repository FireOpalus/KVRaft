package transport

import (
	"net/rpc"
	"sync"
)

type ClientEnd struct {
	addr   string
	client *rpc.Client
	mu     sync.Mutex
}

func MakeClientEnd(addr string) *ClientEnd {
	return &ClientEnd{
		addr: addr,
	}
}

func (c *ClientEnd) Call(serviceMethod string, args interface{}, reply interface{}) bool {
	c.mu.Lock()
	if c.client == nil {
		cli, err := rpc.Dial("tcp", c.addr)
		if err != nil {
			c.mu.Unlock()
			return false
		}
		c.client = cli
	}
	client := c.client
	c.mu.Unlock()

	err := client.Call(serviceMethod, args, reply)
	if err == nil {
		return true
	}

	c.mu.Lock()
	if c.client == client {
		client.Close()
		c.client = nil
	}
	c.mu.Unlock()

	return false
}

func (c *ClientEnd) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		c.client.Close()
		c.client = nil
	}
}
