package rpc

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"strings"

	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/internal/pool"
	"github.com/enabokov/backuper/internal/proto/master"
	"github.com/enabokov/backuper/internal/proto/minion"
)

type BackupUnit struct {
	Namespace string
	Table     string
	Timestamp string
}

var Minions map[string]string
var Backups map[string][]BackupUnit
var connPool *pool.Storage

type Master struct{}

func init() {
	connPool = pool.GetPool()

	Minions = make(map[string]string)
	Backups = make(map[string][]BackupUnit)
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

func findLastBackup(minionIP string, minionPort int, db, namespace, tablename string) string {
	key := fmt.Sprintf("%s:%d:%s", minionIP, minionPort, db)
	backups := Backups[key]

	if len(backups) == 0 {
		return "-"
	}

	var latestTimeStamp string

	for _, backup := range backups {
		if strings.Compare(namespace, backup.Namespace) == 0 &&
			strings.Compare(tablename, backup.Table) == 0 {
			latestTimeStamp = backup.Timestamp
		}
	}

	return latestTimeStamp
}

func (s *Master) GetTablesByMinion(ctx context.Context, query *master.QueryTablesByMinion) (*master.ListTableResponse, error) {
	log.Info.Printf("get tables from %s by minion %s:%d",
		query.Db, query.MinionIP, query.MinionPort)

	var (
		tables []*master.ListTableResponse_TableUnit
	)

	conn := connPool.GRPCConnect(ctx, query.MinionIP, int(query.MinionPort), grpc.WithInsecure())
	defer connPool.GRPCDisconnect(conn)
	client := minion.NewMinionClient(conn)
	resp, err := client.GetTables(
		context.Background(),
		&minion.QueryGetTables{
			Db: query.Db,
		})
	if err != nil {
		log.Error.Println(err)
		return nil, err
	}

	for _, tb := range resp.Tables {
		tables = append(tables,
			&master.ListTableResponse_TableUnit{
				Namespace:  tb.Namespace,
				Name:       tb.Name,
				LastBackup: findLastBackup(query.MinionIP, int(query.MinionPort), query.Db, tb.Namespace, tb.Name),
			},
		)
	}

	log.Info.Printf("done: get tables from %s by minion %s:%d",
		query.Db, query.MinionIP, query.MinionPort)

	return &master.ListTableResponse{Tables: tables}, nil
}

func (s *Master) GetBackupsByMinion(ctx context.Context, query *master.QueryBackupsByMinion) (*master.ListBackupResponse, error) {
	var (
		backups []*master.ListBackupResponse_BackupUnit
	)

	key := fmt.Sprintf("%s:%d:%s", query.MinionIP, query.MinionPort, query.Db)

	for _, bk := range Backups[key] {
		backups = append(backups,
			&master.ListBackupResponse_BackupUnit{
				Namespace: bk.Namespace,
				Table:     bk.Table,
				Timestamp: bk.Timestamp,
			})
	}

	return &master.ListBackupResponse{Backups: backups}, nil
}

func (s *Master) InstantBackupByMinion(ctx context.Context, query *master.QueryBackup) (*master.Response, error) {
	log.Info.Printf("instant backup %s %s:%s by minion %s:%d",
		query.Db, query.Namespace, query.Table, query.MinionIP, query.MinionPort)
	conn := connPool.GRPCConnect(ctx, query.MinionIP, int(query.MinionPort), grpc.WithInsecure())
	defer connPool.GRPCDisconnect(conn)

	client := minion.NewMinionClient(conn)
	resp, err := client.StartBackup(context.Background(), &minion.QueryStartBackup{Db: query.Db, Namespace: query.Namespace, Table: query.Table})
	if err != nil {
		log.Error.Println(err)
		return nil, err
	}
	connPool.GRPCDisconnect(conn)

	log.Info.Printf("done: instant backup %s %s:%s by minion %s:%d",
		query.Db, query.Namespace, query.Table, query.MinionIP, query.MinionPort)

	key := fmt.Sprintf("%s:%d:%s", query.MinionIP, query.MinionPort, query.Db)

	Backups[key] = append(Backups[key],
		BackupUnit{
			Namespace: query.Namespace,
			Table:     query.Table,
			Timestamp: resp.Timestamp,
		})

	return &master.Response{Msg: resp.Msg, Timestamp: resp.Timestamp}, nil
}

func (s *Master) ScheduleBackupByMinion(ctx context.Context, query *master.QueryBackup) (*master.Response, error) {
	log.Info.Printf("schedule backup %s %s:%s at %s by minion %s:%d",
		query.Db, query.Namespace, query.Table, query.Timestamp, query.MinionIP, query.MinionPort)
	conn := connPool.GRPCConnect(ctx,
		query.MinionIP, int(query.MinionPort), grpc.WithInsecure())
	defer connPool.GRPCDisconnect(conn)

	client := minion.NewMinionClient(conn)
	resp, err := client.ScheduleBackup(context.Background(),
		&minion.QueryScheduleBackup{Db: query.Db, Namespace: query.Namespace, Table: query.Table, Timestamp: query.Timestamp},
	)
	if err != nil {
		log.Error.Println(err)
		return nil, err
	}

	log.Info.Printf("done: schedule backup %s %s:%s at %s by minion %s:%d",
		query.Db, query.Namespace, query.Table, resp.Timestamp, query.MinionIP, query.MinionPort)

	key := fmt.Sprintf("%s:%d:%s", query.MinionIP, query.MinionPort, query.Db)

	Backups[key] = append(Backups[key],
		BackupUnit{
			Namespace: query.Namespace,
			Table:     query.Table,
			Timestamp: resp.Timestamp,
		})

	return &master.Response{Msg: resp.Msg, Timestamp: resp.Timestamp}, nil
}
