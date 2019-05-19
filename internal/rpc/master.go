package rpc

import (
	"context"

	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/internal/proto/master"
)

var Minions map[string]string

type Master struct{}

func init() {
	Minions = make(map[string]string)
}

func (s *Master) Heartbeat(ctx context.Context, info *master.MinionInfo) (*master.Response, error) {
	log.Info.Println(info)

	if _, ok := Minions[info.Host]; !ok {
		Minions[info.Host] = info.LocalTime
	}

	return &master.Response{Msg: "OK"}, nil
}

func (s *Master) GetAllMinions(ctx context.Context, query *master.Query) (*master.ListMinions, error) {
	var (
		hosts []string
		times []string
	)

	for host, time := range Minions {
		hosts = append(hosts, host)
		times = append(times, time)
	}

	return &master.ListMinions{Host: hosts, Time: times}, nil
}
