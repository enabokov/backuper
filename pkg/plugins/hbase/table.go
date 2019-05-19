package hbase

import (
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/pkg/plugins/globals"
)

func writeAndGetTmpFile(cmds []string) (filename string) {
	const tmpFilename = "_tmp_" + uniqueKey + ".sh"

	err := os.Remove(tmpFilename)
	if err != nil {
		log.Error.Println(err)
	}

	f, err := os.Create(tmpFilename)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if err := f.Chmod(0666); err != nil {
		log.Error.Println(err)
	}

	for _, cmd := range cmds {
		if _, err = f.WriteString(cmd); err != nil {
			log.Error.Println(err)
			return
		}
	}

	return tmpFilename
}

func createTableFromSnapshot(snapshotname string) (string, error) {
	var cmds []string

	cmds = append(cmds, fmt.Sprintf("clone_snapshot '%s', '%s_table'", snapshotname, snapshotname))
	tmpFilename := writeAndGetTmpFile(cmds)

	log.Info.Println("Creating table from snapshot", snapshotname)
	out, err := exec.Command(
		"hbase",
		"shell",
		tmpFilename,
	).Output()
	if err != nil {
		log.Error.Println(err)
		return "", err
	}

	log.Info.Println(string(out))
	return snapshotname + "_table", nil
}

func createSnapshotFromTable(namespace string, tablename string) string {
	if namespace == "" {
		namespace = "non"
	}

	var (
		t            = time.Now()
		snapshotName = fmt.Sprintf("%s-%s-snapshot-%s-%s", namespace, tablename, uniqueKey, t.Format("2006-01-02-15-04-05"))
	)

	log.Info.Println("Creating snapshot", snapshotName)
	out, err := exec.Command(
		"hbase",
		"org.apache.hadoop.hbase.snapshot.CreateSnapshot",
		fmt.Sprintf("--table %s", tablename),
		fmt.Sprintf("--name %s", snapshotName)).Output()
	if err != nil {
		log.Error.Println(err)
		return ""
	}

	log.Info.Println(string(out))
	return snapshotName
}

func backupTableToS3(socket globals.Socket, table string, options globals.S3Options) {
	snapshotname := createSnapshotFromTable("", table)

}