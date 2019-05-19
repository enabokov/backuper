package rpc

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

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

func init() {
	c = config.InjectStorage
}

func (m *Minion) GetNamespaces(ctx context.Context, query *minion.Query) (*minion.Namespaces, error) {
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

func (m *Minion) GetTables(ctx context.Context, query *minion.Query) (*minion.Tables, error) {
	log.Info.Println(query)

	var (
		// input
		sock  interface{}
		ctxDb StrategyCli

		// output
		tables []string
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

	tables = ctxDb.getDatabase().GetTables(sock)
	return &minion.Tables{Tables: tables}, nil
}

func (m *Minion) StartBackup(ctx context.Context, query *minion.Query) (*minion.Response, error) {
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

	go ctxDb.getDatabase().BackupTableToS3(sock, namespace, tablename, s3dst)
	msg = fmt.Sprintf("Backup %s %s -> %s", namespace, tablename, s3dst.(globals.S3Options).BucketName)
	return &minion.Response{Msg: msg}, nil
}

func (m *Minion) StopBackup(ctx context.Context, query *minion.Query) (*minion.Response, error) {
	return &minion.Response{Msg: query.Namespace + " backup stopped"}, nil
}
