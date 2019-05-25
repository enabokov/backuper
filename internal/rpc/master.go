package rpc

import (
	"context"
	"fmt"
	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/internal/pool"
	"github.com/enabokov/backuper/internal/proto/master"
	"github.com/enabokov/backuper/internal/proto/minion"
	"google.golang.org/grpc"
	"strings"
	"time"
)

type BackupUnit struct {
	Namespace string
	Table     string
	Timestamp string
}

var Minions map[string]string
var Backups map[string][]BackupUnit
var connPool *pool.Storage

var BackupsSchedule map[string]string

type Master struct{}

func init() {
	connPool = pool.GetPool()

	Minions = make(map[string]string)
	Backups = make(map[string][]BackupUnit)
	BackupsSchedule = make(map[string]string)
}

func (s *Master) Heartbeat(ctx context.Context, info *master.MinionInfo) (*master.Response, error) {
	log.Info.Println(info)
	Minions[info.Host] = info.LocalTime
	return &master.Response{Msg: "OK"}, nil
}

func (s *Master) GetAllMinions(ctx context.Context, query *master.Query) (*master.ListMinions, error) {
	var minions []*master.ListMinions_MinionUnit

	for host, t := range Minions {
		var isActive bool

		parsedTime, err := time.Parse(time.RFC850, t)
		if err != nil {
			log.Warn.Println("failed to parse time")
			continue
		}

		period := time.Now().Sub(parsedTime)
		if period.Minutes() < 5 {
			isActive = true
		}

		minions = append(
			minions,
			&master.ListMinions_MinionUnit{
				Host:     host,
				Time:     t,
				IsActive: isActive,
			},
		)
	}

	return &master.ListMinions{Unit: minions}, nil
}

func findLastBackup(minionIP string, minionPort int, db, namespace, tablename string) string {
	key := fmt.Sprintf("%s:%d:%s", minionIP, minionPort, db)
	backups := Backups[key]

	var latestTimeStamp string
	latestTimeStamp = `-`

	if len(backups) == 0 {
		return latestTimeStamp
	}

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
		key := fmt.Sprintf("%s:%s:%s:%s:%s", query.MinionIP, query.MinionPort, query.Db, tb.Namespace, tb.Name)
		tables = append(tables,
			&master.ListTableResponse_TableUnit{
				Namespace:   tb.Namespace,
				Name:        tb.Name,
				ScheduledAt: BackupsSchedule[key],
				LastBackup:  findLastBackup(query.MinionIP, int(query.MinionPort), query.Db, tb.Namespace, tb.Name),
			},
		)
	}

	log.Info.Printf("done: get tables from %s by minion %s:%d",
		query.Db, query.MinionIP, query.MinionPort)

	return &master.ListTableResponse{Tables: tables}, nil
}

func (s *Master) GetBackupsByMinion(ctx context.Context, query *master.QueryBackupsByMinion) (*master.ListBackupResponse, error) {
	var (
		backups []*master.BackupUnit
	)

	key := fmt.Sprintf("%s:%d:%s", query.MinionIP, query.MinionPort, query.Db)

	for _, bk := range Backups[key] {
		backups = append(backups,
			&master.BackupUnit{
				Namespace: bk.Namespace,
				Table:     bk.Table,
				Timestamp: bk.Timestamp,
			})
	}

	return &master.ListBackupResponse{Backups: backups}, nil
}

func (s *Master) UpdateInfoBackup(ctx context.Context, query *master.QueryBackupUnit) (*master.Response, error) {
	log.Info.Printf("update info from %s:%d about %s %s:%s at %s\n",
		query.MinionIP, query.MinionPort, query.Db, query.Unit.Namespace, query.Unit.Table, query.Unit.Timestamp)
	key := fmt.Sprintf("%s:%d:%s", query.MinionIP, query.MinionPort, query.Db)

	Backups[key] = append(Backups[key],
		BackupUnit{
			Namespace: query.Unit.Namespace,
			Table:     query.Unit.Table,
			Timestamp: query.Unit.Timestamp,
		})

	log.Info.Printf("done: update info from %s:%d about %s %s:%s at %s\n",
		query.MinionIP, query.MinionPort, query.Db, query.Unit.Namespace, query.Unit.Table, query.Unit.Timestamp)
	return &master.Response{Msg: "OK", Timestamp: ""}, nil
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

	key := fmt.Sprintf("%s:%s:%s:%s:%s", query.MinionIP, query.MinionPort, query.Db, query.Namespace, query.Table)
	BackupsSchedule[key] = query.Timestamp

	log.Info.Printf("done: schedule backup %s %s:%s at %s by minion %s:%d",
		query.Db, query.Namespace, query.Table, resp.Timestamp, query.MinionIP, query.MinionPort)

	return &master.Response{Msg: resp.Msg, Timestamp: resp.Timestamp}, nil
}
