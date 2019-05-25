package hbase

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/enabokov/backuper/internal/log"
)

func listSnapshots() []string {
	name := filepath.Join(os.Getenv(`PATH_HBASE`), `hbase`)
	out, err := exec.Command(
		name,
		"org.apache.hadoop.hbase.snapshot.SnapshotInfo",
		"-list-snapshots",
	).Output()
	if err != nil {
		log.Error.Println(err)
		return nil
	}

	re := regexp.MustCompile(fmt.Sprintf(".*%s[^\\s]*", uniqueKey))
	return re.FindAllString(string(out), -1)
}

func createSnapshotFromTable(namespace, tablename *string) string {
	var snapshotName string

	if snapshotName == `-` {
		snapshotName = fmt.Sprintf("%s-snapshot-%s", *tablename, uniqueKey)
	} else {
		snapshotName = fmt.Sprintf("%s-%s-snapshot-%s", *namespace, *tablename, uniqueKey)
	}

	name := filepath.Join(os.Getenv(`PATH_HBASE`), `hbase`)
	log.Info.Printf("create snapshot %s from %s\n", snapshotName, *tablename)
	_, err := exec.Command(
		name,
		"org.apache.hadoop.hbase.snapshot.CreateSnapshot",
		fmt.Sprintf("--table %s", *tablename),
		fmt.Sprintf("--name %s", snapshotName)).Output()
	if err != nil {
		return ""
	}

	log.Info.Printf("done: create snapshot %s from %s\n", snapshotName, *tablename)
	return snapshotName
}

func deleteSnapshot(snapshotname *string) {
	cmds := []string{
		fmt.Sprintf("delete_snapshot '%s'", *snapshotname),
	}

	tmpFilename := writeAndGetTmpFile(cmds)

	name := filepath.Join(os.Getenv(`PATH_HBASE`), `hbase`)
	log.Info.Printf("delete snapshot %s\n", *snapshotname)
	_, err := exec.Command(
		name,
		"shell",
		tmpFilename,
	).Output()
	if err != nil {
		log.Error.Printf("failed to delete snapshot %s: %v\n", *snapshotname, err)
	}

	log.Info.Printf("done: delete snapshot %s\n", *snapshotname)
}
