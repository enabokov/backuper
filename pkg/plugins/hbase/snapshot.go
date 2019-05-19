package hbase

import (
	"fmt"
	"github.com/enabokov/backuper/internal/log"
	"os/exec"
	"regexp"
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

	log.Info.Println("Start: create snapshot", snapshotName)
	out, err := exec.Command(
		"hbase",
		"org.apache.hadoop.hbase.snapshot.CreateSnapshot",
		fmt.Sprintf("--table %s", *tablename),
		fmt.Sprintf("--name %s", snapshotName)).Output()
	if err != nil {
		log.Error.Println(err)
		log.Error.Printf("failed to create snapshot %s from table %s\n", snapshotName, *tablename)
		return ""
	}

	log.Info.Println("Done: create snapshot", snapshotName)
	log.Info.Println(string(out))
	return snapshotName
}

func deleteSnapshot(snapshotname *string) {
	cmds := []string{
		fmt.Sprintf("delete_snapshot '%s'", *snapshotname),
	}

	tmpFilename := writeAndGetTmpFile(cmds)

	log.Info.Println("Start: delete snapshot", *snapshotname)
	out, err := exec.Command(
		"hbase",
		"shell",
		tmpFilename,
	).Output()
	if err != nil {
		log.Error.Printf("failed to delete snapshot %s: %v\n", *snapshotname, err)
	}

	log.Info.Println("Done: delete snapshot", *snapshotname)
	log.Info.Println(string(out))
}
