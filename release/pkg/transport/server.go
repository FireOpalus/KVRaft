package transport

import (
	"log"
	"net"
	"net/rpc"
)

type Server struct {
	rpcServer *rpc.Server
	listener  net.Listener
}

func NewServer() *Server {
	return &Server{
		rpcServer: rpc.NewServer(),
	}
}

func (s *Server) Register(rcvr interface{}) error {
	return s.rpcServer.Register(rcvr)
}

func (s *Server) RegisterName(name string, rcvr interface{}) error {
	return s.rpcServer.RegisterName(name, rcvr)
}

// Start starts the RPC server listening on the given address (e.g. ":1234")
// It uses raw TCP.
func (s *Server) Start(addr string) error {
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				// Listener closed or error
				return
			}
			go s.rpcServer.ServeConn(conn)
		}
	}()

	log.Printf("RPC Server listening on %s", addr)
	return nil
}

func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
