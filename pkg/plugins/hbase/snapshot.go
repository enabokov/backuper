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
func deleteSnapshot(snapshotname string) string {
	return ""
}
