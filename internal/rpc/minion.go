package rpc

import (
	"context"
	"fmt"
	"path/filepath"

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

	conf := c.GetMinionConf()
	src := hbase.SourceFile{
		HostNameNode: fmt.Sprintf("%s:%d", conf.NameNode.Host, conf.NameNode.Port),
		Filename:     query.Query,
	}

	dirNames, dirSizes := hbase.GetNamespaces(src)
	return &minion.Namespaces{Names: dirNames, Sizes: dirSizes}, nil
}

func (m *Minion) GetTables(ctx context.Context, query *minion.Query) (*minion.Tables, error) {
	return &minion.Tables{Tables: []string{"some tables"}}, nil
}

func (m *Minion) StartBackup(ctx context.Context, query *minion.Query) (*minion.Response, error) {
	log.Info.Println(query)

	conf := c.GetMinionConf()

	src := hbase.SourceFile{
		HostNameNode: fmt.Sprintf("%s:%d", conf.NameNode.Host, conf.NameNode.Port),
		Filename:     query.Query,
	}

	dst := hbase.TargetS3{
		Region:     conf.Targets.S3.Bucket.Region,
		BucketName: conf.Targets.S3.Bucket.Name,
		Key:        filepath.Join(`backup_hbase`, service.GetPrivateIP()),
	}

	go hbase.CopyToS3Bucket(src, dst)
	return &minion.Response{Msg: query.Query + " backup started"}, nil
}

func (m *Minion) StopBackup(ctx context.Context, query *minion.Query) (*minion.Response, error) {
	return &minion.Response{Msg: query.Query + " backup stopped"}, nil
}
