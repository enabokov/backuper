package main

import (
	"github.com/enabokov/backuper/internal/config"
	"github.com/enabokov/backuper/internal/jobs"
	"github.com/enabokov/backuper/internal/proto/minion"
	"github.com/enabokov/backuper/internal/rpc"
	"github.com/enabokov/backuper/internal/service"
	"google.golang.org/grpc"
	"runtime"
)

var c config.ConfMinion

func init() {
	config.InjectStorage.Put("configs/minion.yaml", `minion`, &c)

	// cron job
	go jobs.Heartbeat(c.Master.Host, c.Master.Port, c.Port, c.Heartbeat)
}

func main() {
	runtime.GOMAXPROCS(1)

	server := grpc.NewServer()
	minion.RegisterMinionServer(server, &rpc.Minion{})

	if err := service.Run(server, c.Port); err != nil {
		panic(err)
	}
}
