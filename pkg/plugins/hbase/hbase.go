package hbase

import (
	"github.com/enabokov/backuper/pkg/plugins/globals"
)

const uniqueKey = "managed-by-backuper"

type HBase struct{}

func (db *HBase) ListSnapshots() []string {
	return listSnapshots()
}

func (db *HBase) GetNamespaces(socket interface{}) (names, sizes []string, checksums []float64) {
	return getNamespaces(socket.(globals.Socket))
}

func (db *HBase) GetTables(socket interface{}) []string {
	return getTables(socket.(globals.Socket))
}

func (db *HBase) BackupTableToS3(socket interface{}, namespace, tablename string, s3 interface{}) {
	backupTableToS3(socket.(globals.Socket), namespace, tablename, s3.(globals.S3Options))
}
