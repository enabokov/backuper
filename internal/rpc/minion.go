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
	"github.com/enabokov/backuper/pkg/plugins/hbase"
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

		sock = hbase.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		ctxDb.setDatabase(&hbase.HBase{})
		break
	case `postgres`:
		//ctxDb.setDatabase(&postgres.Postgres{})
		break
	}

	dirNames, dirSizes, ok = ctxDb.getDatabase().GetNamespaces(sock)
	return &minion.Namespaces{Names: dirNames, Sizes: dirSizes, Ok: ok}, nil
}

func (m *Minion) GetTables(ctx context.Context, query *minion.Query) (*minion.Tables, error) {
	return &minion.Tables{Tables: []string{"some tables"}}, nil
}

func (m *Minion) StartBackup(ctx context.Context, query *minion.Query) (*minion.Response, error) {
	log.Info.Println(query)

	var (
		// input
		sock  interface{}
		s3dst interface{}
		ctxDb StrategyCli

		// output
		msg         string
		srcFilename string
	)

	switch query.Db {
	case `hbase`:
		conf := c.GetMinionConf()

		sock = hbase.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		// TODO: add check in case sql injections
		srcFilename = query.Query

		s3dst = hbase.S3Options{
			Region:     conf.Targets.S3.Bucket.Region,
			BucketName: conf.Targets.S3.Bucket.Name,
			Key:        filepath.Join(`backup_hbase`, service.GetPrivateIP()),
		}

		ctxDb.setDatabase(&hbase.HBase{})
		break
	case `postgres`:
		//ctxDb.setDatabase(&postgres.Postgres{})
		break
	}

	go ctxDb.getDatabase().CopyToS3Bucket(sock, srcFilename, s3dst)
	msg = fmt.Sprintf("Backup %s -> %s", srcFilename, s3dst)
	return &minion.Response{Msg: msg}, nil
}

func (m *Minion) StopBackup(ctx context.Context, query *minion.Query) (*minion.Response, error) {
	return &minion.Response{Msg: query.Query + " backup stopped"}, nil
}
