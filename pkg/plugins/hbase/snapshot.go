package hbase

import (
	"fmt"
	"os/exec"
	"regexp"

	"github.com/enabokov/backuper/internal/log"
)

func listSnapshots() []string {
	out, err := exec.Command(
		"hbase",
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
	var snapshotName = fmt.Sprintf("%s-%s-snapshot-%s", *namespace, *tablename, uniqueKey)

	log.Info.Printf("create snapshot %s from %s\n", snapshotName, *tablename)
	_, err := exec.Command(
		"hbase",
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

	log.Info.Printf("delete snapshot %s\n", *snapshotname)
	_, err := exec.Command(
		"hbase",
		"shell",
		tmpFilename,
	).Output()
	if err != nil {
		log.Error.Printf("failed to delete snapshot %s: %v\n", *snapshotname, err)
	}

	log.Info.Printf("done: delete snapshot %s\n", *snapshotname)
}
