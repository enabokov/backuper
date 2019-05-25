package rpc

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/enabokov/backuper/internal/config"
	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/internal/proto/minion"
	"github.com/enabokov/backuper/internal/service"
	"github.com/enabokov/backuper/pkg/plugins/globals"
	"github.com/enabokov/backuper/pkg/plugins/hbase"
	"github.com/enabokov/backuper/pkg/plugins/postgres"
)

var (
	c config.Storage
)

const timeFormat = `Mon Jan _2 15:04:05 2006`

func init() {
	c = config.InjectStorage
}

func (m *Minion) GetNamespaces(ctx context.Context, query *minion.QueryGetNamespaces) (*minion.Namespaces, error) {
	log.Info.Println(query)

	var (
		// input
		sock  interface{}
		ctxDb StrategyCli

		// output
		dirNames []string
		dirSizes []string
		ok       []float64
	)

	switch query.Db {
	case `hbase`:
		conf := c.GetMinionConf()

		sock = globals.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		ctxDb.setDatabase(&hbase.HBase{})
		break
	case `postgres`:
		conf := c.GetMinionConf()
		sock = globals.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		ctxDb.setDatabase(&postgres.Postgres{})
		break
	}

	dirNames, dirSizes, ok = ctxDb.getDatabase().GetNamespaces(sock)
	return &minion.Namespaces{Names: dirNames, Sizes: dirSizes, Ok: ok}, nil
}

func (m *Minion) GetTables(ctx context.Context, query *minion.QueryGetTables) (*minion.Tables, error) {
	log.Info.Println(query)

	var (
		// input
		sock  interface{}
		ctxDb StrategyCli

		// output
		tables     []*minion.Tables_TableUnit
		namespace  string
		tablename  string
		lastbackup string
	)

	switch query.Db {
	case `hbase`:
		conf := c.GetMinionConf()

		sock = globals.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		ctxDb.setDatabase(&hbase.HBase{})
		break
	case `postgres`:
		conf := c.GetMinionConf()
		sock = globals.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		ctxDb.setDatabase(&postgres.Postgres{})
		break
	}

	tablesList := ctxDb.getDatabase().GetTables(sock)
	for _, tb := range tablesList {
		if !strings.Contains(tb, ":") {
			tablename = tb
		} else {
			parts := strings.Split(tb, ":")
			if len(parts) > 1 {
				namespace = parts[0]
				tablename = parts[1]
			} else {
				namespace = "none"
				tablename = parts[0]
			}
		}

		tables = append(tables,
			&minion.Tables_TableUnit{
				Name:       tablename,
				Namespace:  namespace,
				LastBackup: lastbackup,
			})
	}

	return &minion.Tables{Tables: tables}, nil
}

func (m *Minion) GetBackups(ctx context.Context, query *minion.QueryDatabase) (*minion.Backups, error) {
	return &minion.Backups{Backups: []string{}}, nil
}

func (m *Minion) StartBackup(ctx context.Context, query *minion.QueryStartBackup) (*minion.Response, error) {
	log.Info.Println(query)

	var (
		// input
		sock  interface{}
		s3dst interface{}
		ctxDb StrategyCli

		// output
		msg       string
		namespace string
		tablename string
		timestamp string
	)

	switch query.Db {
	case `hbase`:
		conf := c.GetMinionConf()

		sock = globals.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		namespace = query.Namespace
		tablename = query.Table

		s3dst = globals.S3Options{
			Region:     conf.Targets.S3.Bucket.Region,
			BucketName: conf.Targets.S3.Bucket.Name,
			Key:        filepath.Join(`backup_hbase`, service.GetPrivateIP()),
		}

		ctxDb.setDatabase(&hbase.HBase{})
		break
	case `postgres`:
		conf := c.GetMinionConf()
		s3dst = globals.S3Options{
			Region:     conf.Targets.S3.Bucket.Region,
			BucketName: conf.Targets.S3.Bucket.Name,
			Key:        filepath.Join(`backup_postgres`, service.GetPrivateIP()),
		}

		ctxDb.setDatabase(&postgres.Postgres{})
		break
	}

	go ctxDb.getDatabase().BackupInstant(sock, namespace, tablename, s3dst)

	timestamp = time.Now().Format(timeFormat)
	msg = fmt.Sprintf("Backup %s %s -> %s at %s", namespace, tablename, s3dst.(globals.S3Options).BucketName, timestamp)
	return &minion.Response{Msg: msg, Timestamp: timestamp}, nil
}

func (m *Minion) ScheduleBackup(ctx context.Context, query *minion.QueryScheduleBackup) (*minion.Response, error) {
	log.Info.Println(query)

	var (
		// input
		sock  interface{}
		s3dst interface{}
		ctxDb StrategyCli

		// output
		msg       string
		namespace string
		tablename string
		date      string
		timestamp string
	)

	switch query.Db {
	case `hbase`:
		conf := c.GetMinionConf()

		sock = globals.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		namespace = query.Namespace
		tablename = query.Table
		timestamp = query.Timestamp

		s3dst = globals.S3Options{
			Region:     conf.Targets.S3.Bucket.Region,
			BucketName: conf.Targets.S3.Bucket.Name,
			Key:        filepath.Join(`backup_hbase`, service.GetPrivateIP()),
		}

		ctxDb.setDatabase(&hbase.HBase{})
		break
	case `postgres`:
		conf := c.GetMinionConf()
		s3dst = globals.S3Options{
			Region:     conf.Targets.S3.Bucket.Region,
			BucketName: conf.Targets.S3.Bucket.Name,
			Key:        filepath.Join(`backup_postgres`, service.GetPrivateIP()),
		}

		ctxDb.setDatabase(&postgres.Postgres{})
		break
	}

	ctxDb.getDatabase().BackupSchedule(sock, namespace, tablename, timestamp, s3dst)

	date = time.Now().Format(timeFormat)
	msg = fmt.Sprintf("Scheduled backup %s:%s every %s", query.Namespace, query.Table, timestamp)
	return &minion.Response{Msg: msg, Timestamp: date}, nil
}

func (m *Minion) UnscheduleBackup(ctx context.Context, query *minion.QueryScheduleBackup) (*minion.Response, error) {
	return &minion.Response{Msg: query.Namespace + " backup stopped"}, nil
}
