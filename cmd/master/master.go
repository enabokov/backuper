package main

import (
	"google.golang.org/grpc"

	"github.com/enabokov/backuper/internal/config"
	"github.com/enabokov/backuper/internal/proto/master"
	"github.com/enabokov/backuper/internal/rpc"
	"github.com/enabokov/backuper/internal/service"
)

var c config.ConfMaster

func init() {
	config.InjectStorage.Put("configs/master.yaml", `master`, &c)
}

func main() {
	server := grpc.NewServer()
	master.RegisterMasterServer(server, &rpc.Master{})

	if err := service.Run(server, c.Port); err != nil {
		panic(err)
	}
}
