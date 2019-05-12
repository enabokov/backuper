package hbase

import (
	"github.com/colinmarc/hdfs"
	"github.com/enabokov/backuper/internal/log"
)

func getHDFSClient(src SourceFile) *hdfs.Client {
	client, err := hdfs.New(src.HostNameNode)
	if err != nil {
		log.Error.Println(err)
		return nil
	}

	return client
}
