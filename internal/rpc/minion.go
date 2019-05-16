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

type Minion struct{}

var c config.Storage

func init() {
	c = config.InjectStorage
}

func (m *Minion) GetNamespaces(ctx context.Context, query *minion.Query) (*minion.Namespaces, error) {
	log.Info.Println(query)

	var (
		dirNames []string
		dirSizes []string
		ok       []float64
	)

	conf := c.GetMinionConf()

	switch query.Db {
	case `hbase`:
		sock := hbase.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		dirNames, dirSizes, ok = hbase.GetNamespaces(sock)
		break
	case `postgres`:
		break
	case `mongodb`:
		break
	case `cassandra`:
		break
	}

	return &minion.Namespaces{Names: dirNames, Sizes: dirSizes, Ok: ok}, nil
}

func (m *Minion) GetTables(ctx context.Context, query *minion.Query) (*minion.Tables, error) {
	return &minion.Tables{Tables: []string{"some tables"}}, nil
}

func (m *Minion) StartBackup(ctx context.Context, query *minion.Query) (*minion.Response, error) {
	log.Info.Println(query)

	var (
		msg string
	)

	conf := c.GetMinionConf()

	switch query.Db {
	case `hbase`:
		sock := hbase.Socket{
			IP:   conf.NameNode.Host,
			Port: strconv.Itoa(conf.NameNode.Port),
		}

		// TODO: add check in case sql injections
		srcFilename := query.Query

		dst := hbase.S3Options{
			Region:     conf.Targets.S3.Bucket.Region,
			BucketName: conf.Targets.S3.Bucket.Name,
			Key:        filepath.Join(`backup_hbase`, service.GetPrivateIP()),
		}

		go hbase.CopyToS3Bucket(sock, srcFilename, dst)
		msg = fmt.Sprintf("Backup %s -> %s", srcFilename, filepath.Join(dst.BucketName, dst.Key))
		break
	case `postgres`:
		msg = "Not implemented yet"
		break
	case `mongodb`:
		msg = "Not implemented yet"
		break
	case `cassandra`:
		msg = "Not implemented yet"
		break
	}

	return &minion.Response{Msg: msg}, nil
}

func (m *Minion) StopBackup(ctx context.Context, query *minion.Query) (*minion.Response, error) {
	return &minion.Response{Msg: query.Query + " backup stopped"}, nil
}
