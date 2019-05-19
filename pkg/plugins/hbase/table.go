package hbase

import (
	"bufio"
	"fmt"
	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/pkg/plugins/globals"
	"os"
	"os/exec"
	"regexp"
	"strings"
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
	cmds := []string{
		fmt.Sprintf("clone_snapshot '%s', '%s_table'", snapshotname, snapshotname),
	}
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

func getTables(socket globals.Socket) (tables []string) {
	cmds := []string{`list`}
	tmpFilename := writeAndGetTmpFile(cmds)

	log.Info.Println("Getting list of tables from hbase")
	out, err := exec.Command(
		"hbase",
		"shell",
		tmpFilename,
	).Output()
	if err != nil {
		log.Error.Println(err)
		return nil
	}

	reader := bufio.NewReader(
		strings.NewReader(string(out)))
	re := regexp.MustCompile(fmt.Sprintf("^[a-zA-Z0-9:_.-]*"))
	for {
		line, err := reader.ReadString('\n')
		line = re.FindString(line)
		tables = append(tables, line)
		if err != nil {
			break
		}
	}

	return tables
}

func backupTableToS3(socket globals.Socket, namespace *string, table *string, options *globals.S3Options) {
	snapshotname := createSnapshotFromTable(namespace, table)
	ok := uploadSnapshotToS3(&snapshotname, options)
	if !ok {
		log.Error.Printf("failed to upload snapshot %s\n", snapshotname)
	}
	deleteSnapshot(&snapshotname)
}
