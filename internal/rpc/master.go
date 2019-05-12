package rpc

import (
	"context"

	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/internal/proto/master"
)

var Minions map[string]int

type Master struct{}

func init() {
	Minions = make(map[string]int)
}

func (s *Master) Heartbeat(ctx context.Context, info *master.MinionInfo) (*master.Response, error) {
	log.Info.Println(info)

	if _, ok := Minions[info.Host]; !ok {
		Minions[info.Host] = 1
	}

	return &master.Response{Msg: "OK"}, nil
}

func (s *Master) GetAllMinions(ctx context.Context, query *master.Query) (*master.ListMinions, error) {
	log.Info.Println(query)

	var hosts []string
	for host := range Minions {
		hosts = append(hosts, host)
	}

	return &master.ListMinions{Host: hosts}, nil
}
