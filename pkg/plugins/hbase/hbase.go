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

func (db *HBase) BackupSchedule(socket interface{}, namespace, tablename, timestamp string, s3 interface{}) {
	s3a := s3.(globals.S3Options)
	backupSchedule(socket.(globals.Socket), &namespace, &tablename, &timestamp, &s3a)
}

func (db *HBase) BackupUnschedule(socket interface{}, namespace, tablename, timestamp string, s3 interface{}) {
	s3a := s3.(globals.S3Options)
	backupSchedule(socket.(globals.Socket), &namespace, &tablename, &timestamp, &s3a)
}

func (db *HBase) BackupTableToS3(socket interface{}, namespace, tablename string, s3 interface{}) {
	s3a := s3.(globals.S3Options)
	backupTableToS3(socket.(globals.Socket), &namespace, &tablename, &s3a)
}
