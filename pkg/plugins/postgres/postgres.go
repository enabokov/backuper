package postgres

import (
	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/pkg/plugins/globals"
)

type Postgres struct{}

func (db *Postgres) ListSnapshots() []string {
	log.Error.Println("not implemented")
	return nil
}

func (db *Postgres) GetNamespaces(socket interface{}) (names, sizes []string, checksums []float64) {
	log.Error.Println("not implemented")
	return nil, nil, nil
}

func (db *Postgres) GetTables(socket interface{}) []string {
	log.Error.Println("not implemented")
	return nil
}

func (db *Postgres) BackupSchedule(socket interface{}, namespace, tablename, timestamp string, s3 interface{}) {
	socket = socket.(globals.Socket)
	s3 = s3.(globals.S3Options)
	log.Error.Println("not implemented")
}

func (db *Postgres) BackupInstant(socket interface{}, namespace, tablename string, s3 interface{}) {
	socket = socket.(globals.Socket)
	s3 = s3.(globals.S3Options)
	log.Error.Println("not implemented")
}
