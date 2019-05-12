package pool

import (
	"context"
	"fmt"
	"github.com/enabokov/backuper/internal/log"
	"google.golang.org/grpc"
)

type Storage struct {
	pool     map[context.Context]*grpc.ClientConn
	capacity uint8
}

func (s *Storage) GRPCConnect(ctx context.Context, host string, port int, opts ...grpc.DialOption) *grpc.ClientConn {
	conn, ok := s.pool[ctx]
	if ok {
		s.capacity -= 1
		return conn
	}

	target := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.DialContext(ctx, target, opts...)
	if err != nil {
		log.Error.Println(err)
		return nil
	}

	return conn
}

func (s *Storage) GRPCDisconnect(conn *grpc.ClientConn) {
	if s.capacity+1 > 3 {
		if err := conn.Close(); err != nil {
			return
		}

		return
	}

	s.capacity += 1
}

func getPool() *Storage {
	pool := Storage{
		pool: make(map[context.Context]*grpc.ClientConn),
	}

	return &pool
}

func GetPool() *Storage {
	return getPool()
}
