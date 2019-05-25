package hbase

import (
	"bufio"
	"context"
	"fmt"
	"github.com/enabokov/backuper/internal/config"
	"github.com/enabokov/backuper/internal/proto/master"
	"github.com/enabokov/backuper/internal/service"
	"google.golang.org/grpc"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
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
	cmds := []string{
		fmt.Sprintf("clone_snapshot '%s', '%s_table'", snapshotname, snapshotname),
	}
	tmpFilename := writeAndGetTmpFile(cmds)

	name := filepath.Join(os.Getenv(`PATH_HBASE`), `hbase`)
	log.Info.Println("create table from snapshot", snapshotname)
	_, err := exec.Command(
		name,
		"shell",
		tmpFilename,
	).Output()
	if err != nil {
		log.Error.Println(err)
		return "", err
	}

	log.Info.Println("done: create table from snapshot", snapshotname)
	return snapshotname + "_table", nil
}

func getTables(socket globals.Socket) (tables []string) {
	cmds := []string{`list`}
	tmpFilename := writeAndGetTmpFile(cmds)

	var (
		out []byte
		err error
	)

	name := filepath.Join(os.Getenv(`PATH_HBASE`), `hbase`)
	log.Info.Println("get list tables' names from hbase")
	out, err = exec.Command(
		name,
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
		if err != nil {
			break
		}

		line = re.FindString(line)
		if strings.EqualFold(line, "TABLE") ||
			strings.EqualFold(line, "147") ||
			strings.EqualFold(line, "") ||
			strings.EqualFold(line, "HBase") ||
			strings.EqualFold(line, "Type") ||
			strings.EqualFold(line, "Version") {
			continue
		}

		tables = append(tables, line)
	}
	log.Info.Println("done: get list tables' names from hbase")
	return tables
}

func backupInstant(socket globals.Socket, namespace *string, table *string, options *globals.S3Options) {
	snapshotname := createSnapshotFromTable(namespace, table)
	if snapshotname == "" {
		log.Error.Printf("failed to create snapshot from %s:%s\n", *namespace, *table)
		return
	}

	if ok := uploadSnapshotToS3(&snapshotname, options); !ok {
		log.Error.Printf("failed to upload snapshot %s\n", snapshotname)
		return
	}

	deleteSnapshot(&snapshotname)
	updateMasterBackupList(*namespace, *table)
}

func updateMasterBackupList(namespace, tablename string) {
	c := config.InjectStorage
	minionConf := c.GetMinionConf()

	target := fmt.Sprintf("%s:%d", minionConf.Master.Host, minionConf.Master.Port)
	conn, err := grpc.DialContext(context.Background(), target, grpc.WithInsecure())
	if err != nil {
		log.Info.Println("failed to update master backups", err)
		return
	}

	localIP := service.GetPrivateIP()
	if localIP == "" {
		log.Info.Println("failed to update master backups", err)
		return
	}

	client := master.NewMasterClient(conn)
	resp, err := client.UpdateInfoBackup(
		context.Background(),
		&master.QueryBackupUnit{
			MinionIP:   localIP,
			MinionPort: int64(minionConf.Port),
			Db:         `hbase`,
			Unit: &master.BackupUnit{
				Namespace: namespace,
				Table:     tablename,
				Timestamp: time.Now().Format(time.RFC850),
			},
		},
	)

	if err != nil {
		log.Error.Println(err)
		return
	}

	log.Info.Println(resp.Msg)
}
