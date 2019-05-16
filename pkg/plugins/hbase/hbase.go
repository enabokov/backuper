package hbase

import (
	"fmt"
	"github.com/enabokov/backuper/internal/log"
	"os"
	"os/exec"
	"regexp"
	"time"
)

type Socket struct {
	IP string
	Port string
}

func (s *Socket) GetHost() string {
	return fmt.Sprintf("%s:%s", s.IP, s.Port)
}

type S3Options struct {
	Region     string
	BucketName string
	Key        string
}

const uniqueKey = "managed-by-backuper"

func createSnapshot(namespace string, tablename string) string {
	if namespace == "" {
		namespace = "non"
	}

	var (
		t = time.Now()
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

func CreateSnapshot(namespace string, tablename string) string {
	return createSnapshot(namespace, tablename)
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

func DeleteSnapshot(namespace, tablename, timestamp string) string {
	return deleteSnapshot(namespace, tablename, timestamp)
}

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

func ListSnapshots() []string {
	return listSnapshots()
}

func writeAndGetTmpFile(cmds []string) (filename string) {
	const tmpFilename = "_tmp_"+uniqueKey+".sh"

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

func createTableFromSnapshot(snapshotname string)  (string, error) {
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
	return snapshotname+"_table", nil
}

func CreateTableFromSnapshot(snapshotname string) (string, error) {
	return createTableFromSnapshot(snapshotname)
}
