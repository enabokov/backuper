package web

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/enabokov/backuper/internal/config"
	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/internal/pool"
	"github.com/enabokov/backuper/internal/proto/master"
)

type HTMLContext map[string]interface{}

var c config.Storage
var connPool *pool.Storage

type MinionUnit struct {
	IP       string `json:"ip"`
	Port     string `json:"port"`
	Time     string `json:"time"`
	IsActive bool   `json:"isActive"`
}

var minions map[string]MinionUnit

func init() {
	c = config.InjectStorage
	connPool = pool.GetPool()

	minions = make(map[string]MinionUnit)
}

func getIndexPage(w http.ResponseWriter, r *http.Request) {
	log.Info.Println(r.URL.Path)

	conf := c.GetDashboardConf()
	conn := connPool.GRPCConnect(r.Context(), conf.Master.Host, conf.Master.Port, grpc.WithInsecure())
	client := master.NewMasterClient(conn)
	resp, err := client.GetAllMinions(context.Background(), &master.Query{})
	if err != nil {
		log.Error.Println(err)
		return
	}
	connPool.GRPCDisconnect(conn)

	for _, unit := range resp.Unit {
		parts := strings.Split(unit.Host, ":")
		ip := parts[0]
		port := parts[1]

		minions[ip] = MinionUnit{
			IP:       ip,
			Port:     port,
			Time:     unit.Time,
			IsActive: unit.IsActive,
		}
	}

	ctx := HTMLContext{
		"Minions": minions,
	}

	render(w, ctx, "index.html")
}

func backupStart(w http.ResponseWriter, r *http.Request) {
	log.Info.Println(r.URL.Path)

	dashboardConf := c.GetDashboardConf()

	params, err := escapeParams(r, "host", "port", "db", "table", "namespace")
	if err != nil {
		log.Error.Println(err)
		http.Redirect(w, r, "/400", http.StatusFound)
		return
	}

	port, err := strconv.Atoi(params["port"])
	if err != nil {
		log.Error.Println(err)
	}

	log.Info.Printf("trigger master to instant backup %s, %s, %d", params["table"], params["host"], port)
	conn := connPool.GRPCConnect(r.Context(), dashboardConf.Master.Host, dashboardConf.Master.Port, grpc.WithInsecure())
	client := master.NewMasterClient(conn)
	resp, err := client.InstantBackupByMinion(
		context.Background(),
		&master.QueryBackup{
			MinionIP:   params[`host`],
			MinionPort: int64(port),
			Db:         params["db"],
			Namespace:  params["namespace"],
			Table:      params["table"],
			Timestamp:  "",
		},
	)
	if err != nil {
		log.Error.Println(err)
		connPool.GRPCDisconnect(conn)
		return
	}
	connPool.GRPCDisconnect(conn)
	log.Info.Printf("done: trigger master to instant backup %s, %s, %d", params["table"], params["host"], port)

	config.Cache.Set(`alert`, resp.Msg, 1*time.Minute)

	redirectUrl := fmt.Sprintf("/progress?db=%s&&host=%s&&port=%d", params["db"], params["host"], port)
	http.Redirect(w, r, redirectUrl, 302)
}

func backupSchedule(w http.ResponseWriter, r *http.Request) {
	params, err := escapeParams(r, "host", "port", "db", "table", "namespace")
	if err != nil {
		log.Error.Println(err)
		http.Redirect(w, r, "/400", http.StatusFound)
		return
	}
	port, err := strconv.Atoi(params["port"])
	if err != nil {
		log.Error.Println(err)
	}

	dashboardConf := c.GetDashboardConf()

	if r.Method != http.MethodPost {
		redirectUrl := fmt.Sprintf("/progress?db=%s&&host=%s&&port=%d", params["db"], params["host"], port)
		http.Redirect(w, r, redirectUrl, 302)
	}

	forms, _ := escapeForms(r, "schedule-time", "schedule-daily", "schedule-weekly", "schedule-monthly")

	log.Info.Printf("trigger master to schedule backup %s, %s, %d at %s", params["table"], params["host"], port, forms[`schedule-time`])
	conn := connPool.GRPCConnect(r.Context(), dashboardConf.Master.Host, dashboardConf.Master.Port, grpc.WithInsecure())
	client := master.NewMasterClient(conn)
	resp, err := client.ScheduleBackupByMinion(
		context.Background(),
		&master.QueryBackup{
			MinionIP:   params[`host`],
			MinionPort: int64(port),
			Db:         params["db"],
			Namespace:  params["namespace"],
			Table:      params["table"],
			Timestamp:  forms["schedule-time"],
		})

	if err != nil {
		log.Error.Println(err)
		connPool.GRPCDisconnect(conn)
		return
	}
	connPool.GRPCDisconnect(conn)
	log.Info.Printf("done: trigger master to schedule backup %s, %s, %d at %s", params["table"], params["host"], port, forms[`schedule-time`])

	config.Cache.Set(`alert`, resp.Msg, 1*time.Minute)

	redirectUrl := fmt.Sprintf("/progress?db=%s&&host=%s&&port=%d", params["db"], params["host"], port)
	http.Redirect(w, r, redirectUrl, 302)
}

func backupProgress(w http.ResponseWriter, r *http.Request) {
	var (
		tables []map[string]string
		alerts []string
	)

	params, err := escapeParams(r, "db", "host", "port")
	if err != nil {
		log.Error.Println(err)
		http.Redirect(w, r, "/400", http.StatusFound)
		return
	}

	port, err := strconv.Atoi(params[`port`])
	if err != nil {
		log.Error.Println(port)
		return
	}

	dashboardConf := c.GetDashboardConf()
	log.Info.Printf("trigger master to get tables from minion %s, %d", params["host"], port)
	conn := connPool.GRPCConnect(r.Context(), dashboardConf.Master.Host, dashboardConf.Master.Port, grpc.WithInsecure())
	client := master.NewMasterClient(conn)

	_tablesFromCache, ok := config.Cache.Get(params[`db`] + `:tables`)
	if !ok {
		tables, err := client.GetTablesByMinion(context.Background(),
			&master.QueryTablesByMinion{
				MinionIP:   params[`host`],
				MinionPort: int64(port),
				Db:         params[`db`],
			})

		if err != nil {
			log.Error.Println(err)
			return
		}

		var (
			_tmpTables []map[string]string
			ns         string
		)

		for _, tb := range tables.Tables {
			if tb.Namespace == "" {
				ns = "-"
			} else {
				ns = tb.Namespace
			}
			_tmpTables = append(
				_tmpTables,
				map[string]string{
					`namespace`:   ns,
					`tablename`:   tb.Name,
					`lastbackup`:  tb.LastBackup,
					`scheduledAt`: tb.ScheduledAt,
				},
			)
		}

		config.Cache.Set(params[`db`]+`:tables`, _tmpTables, 1*time.Minute)
		_tablesFromCache = _tmpTables
	}

	tables = _tablesFromCache.([]map[string]string)
	connPool.GRPCDisconnect(conn)
	log.Info.Printf("done: trigger master to get tables from minion %s, %d", params["host"], port)

	_alert, _ := config.Cache.Get(`alert`)
	if _alert != nil {
		alerts = append(alerts, _alert.(string))
	}

	ctx := HTMLContext{
		"Db":                params["db"],
		"CurrentMinionTime": minions[params["host"]].Time,
		"Host":              params["host"],
		"Port":              params["port"],
		"Alerts":            alerts,
		"Tables":            tables,
	}

	render(w, ctx, "progress.html")
}

func errorBadRequest(w http.ResponseWriter, r *http.Request) {
	log.Info.Println(r.URL.Path)
	ctx := HTMLContext{}
	render(w, ctx, "errors/400.html")
}

func errorNotFound(w http.ResponseWriter, r *http.Request) {
	log.Info.Println(r.URL.Path)
	ctx := HTMLContext{}
	render(w, ctx, "errors/404.html")
}

func SetupHandlers() {
	http.HandleFunc("/", getIndexPage)
	http.HandleFunc("/start", backupStart)
	http.HandleFunc("/schedule", backupSchedule)
	http.HandleFunc("/progress", backupProgress)

	// error handlers
	http.HandleFunc("/400", errorBadRequest)
	//http.HandleFunc("/401", error401)
	//http.HandleFunc("/403", error403)
	http.HandleFunc("/404", errorNotFound)
}
