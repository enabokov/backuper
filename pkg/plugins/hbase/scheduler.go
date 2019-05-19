package hbase

import (
	"github.com/enabokov/backuper/internal/jobs"
	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/pkg/plugins/globals"
)

func backupSchedule(socket globals.Socket, namespace, tablename, timestamp *string, s3 *globals.S3Options) {
	backupFunc := func() {
		log.Info.Printf("Executed cron job for backup %s:%s to %s every %s\n",
			*namespace, *tablename, s3.BucketName, timestamp)
		backupTableToS3(socket, namespace, tablename, s3)
	}

	log.Info.Printf("Schedule backup %s:%s every %s\n", *namespace, *tablename, *timestamp)
	db := `hbase`
	ok := jobs.ScheduleBackup(&db, namespace, tablename, timestamp, backupFunc)
	if !ok {
		log.Error.Println("failed to schedule backup")
	}
}
