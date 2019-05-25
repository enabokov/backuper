package jobs

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/jasonlvhit/gocron"
	"google.golang.org/grpc"
	"strings"
	"time"

	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/internal/pool"
	"github.com/enabokov/backuper/internal/proto/master"
	"github.com/enabokov/backuper/internal/service"
)

var connPool *pool.Storage
var tasks map[string]*func()

func init() {
	connPool = pool.GetPool()
	tasks = make(map[string]*func())
}

func heartbeat(masterHost string, masterPort int, port int) {
	conn := connPool.GRPCConnect(context.Background(), masterHost, masterPort, grpc.WithInsecure())
	privateIP := service.GetPrivateIP()
	if privateIP == "" {
		log.Error.Println("failed to heartbeat: not found private ip addrs.")
		return
	}

	host := fmt.Sprintf("%s:%d", privateIP, port)
	client := master.NewMasterClient(conn)
	msg, err := client.Heartbeat(
		context.Background(),
		&master.MinionInfo{
			Host:      host,
			LocalTime: time.Now().Format(time.RFC850),
		},
	)

	if err != nil {
		log.Error.Println(err)
		return
	}

	if msg.Msg != "OK" {
		log.Error.Println("response does not matched")
	}

	connPool.GRPCDisconnect(conn)
}

func Heartbeat(masterHost string, masterPort int, port int, interval int64) {
	// first heartbeat without delay
	heartbeat(masterHost, masterPort, port)

	delay := time.Second * time.Duration(interval)
	for range time.NewTicker(delay).C {
		heartbeat(masterHost, masterPort, port)
		log.Info.Println("heartbeat ->", masterHost, masterPort)
	}
}

func scheduleBackup(db, namespace, table, timestamp *string, backupFunc func()) bool {
	parts := strings.Split(*timestamp, ":")

	var (
		hours, minutes string
	)

	if len(parts) > 1 {
		hours = parts[0]
		minutes = parts[1]
		*timestamp = fmt.Sprintf("%s:%s", hours, minutes)
	}

	gocron.Every(1).Day().At(*timestamp).Do(backupFunc)

	hasher := md5.New()
	hasher.Write([]byte(*db))
	hasher.Write([]byte(*namespace))
	hasher.Write([]byte(*table))
	hasher.Write([]byte(*timestamp))
	tasks[hex.EncodeToString(hasher.Sum(nil))] = &backupFunc
	go func() {
		<-gocron.Start()
	}()

	return true
}

func ScheduleBackup(db, namespace, table, timestamp *string, backupFunc func()) bool {
	return scheduleBackup(db, namespace, table, timestamp, backupFunc)
}

func UnScheduleBackup(db, namespace, table, timestamp string) {
	var task *func()

	hasher := md5.New()
	hasher.Write([]byte(db))
	hasher.Write([]byte(namespace))
	hasher.Write([]byte(table))
	hasher.Write([]byte(timestamp))
	task = tasks[hex.EncodeToString(hasher.Sum(nil))]
	gocron.Remove(task)
}

func collect(name *string, timestamp *uint64, infoFunc func()) bool {
	gocron.Every(*timestamp).Minutes().Do(infoFunc)

	hasher := md5.New()
	hasher.Write([]byte(*name))
	tasks[hex.EncodeToString(hasher.Sum(nil))] = &infoFunc
	go func() {
		<-gocron.Start()
	}()

	return true
}

func Collect(name *string, timestamp *uint64, infoFunc func()) bool {
	return collect(name, timestamp, infoFunc)
}
