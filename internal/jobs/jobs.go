package jobs

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"time"

	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/internal/pool"
	"github.com/enabokov/backuper/internal/proto/master"
	"github.com/enabokov/backuper/internal/service"
)

var connPool *pool.Storage

func init() {
	connPool = pool.GetPool()
}

func heartbeat(masterHost string, masterPort int, port int) {
	conn := connPool.GRPCConnect(context.Background(), masterHost, masterPort, grpc.WithInsecure())
	privateIP := service.GetPrivateIP()
	if privateIP == "" {
		log.Error.Println("failed to heartbeat: not found private ip addrs.")
		return
	}

	host := fmt.Sprintf("%s:%d", privateIP, port)

	client := master.NewMasterClient(conn)
	msg, err := client.Heartbeat(
		context.Background(),
		&master.MinionInfo{Host: host},
	)

	if err != nil {
		log.Error.Println(err)
		return
	}

	if msg.Msg != "OK" {
		log.Error.Println("Response does not matched")
	}

	connPool.GRPCDisconnect(conn)
}

func Heartbeat(masterHost string, masterPort int, port int, interval int64) {
	// first heartbeat without delay
	heartbeat(masterHost, masterPort, port)

	delay := time.Second * time.Duration(interval)
	for range time.NewTicker(delay).C {
		heartbeat(masterHost, masterPort, port)
		log.Info.Println("heartbeat ->", masterHost, masterPort)
	}
}
