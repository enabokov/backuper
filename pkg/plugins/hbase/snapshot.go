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

// TODO: correct it
func deleteSnapshot(namespace, tablename, timestamp string) string {
	if namespace == "" {
		namespace = "non"
	}

	var (
		snapshotName = fmt.Sprintf("%s-%s-snapshot-%s-%s", namespace, tablename, uniqueKey, timestamp)
	)

	log.Info.Println("Deleting snapshot", snapshotName)
	out, err := exec.Command(
		"hbase",
		"org.apache.hadoop.hbase.snapshot.DeleteSnapshot",
		fmt.Sprintf("--table %s", tablename),
		fmt.Sprintf("--name %s", snapshotName)).Output()
	if err != nil {
		log.Error.Println(err)
		return ""
	}

	log.Info.Println(string(out))
	return snapshotName
}
