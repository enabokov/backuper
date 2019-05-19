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
	"github.com/enabokov/backuper/internal/proto/minion"
)

type HTMLContext map[string]interface{}

var c config.Storage
var connPool *pool.Storage
var minions map[string]string
var minionsTime map[string]string

func init() {
	c = config.InjectStorage
	connPool = pool.GetPool()

	minions = make(map[string]string)
	minionsTime = make(map[string]string)
}

func getIndexPage(w http.ResponseWriter, r *http.Request) {
	log.Info.Println(r.URL.Path)

	conf := c.GetDashboardConf()
	conn := connPool.GRPCConnect(r.Context(), conf.Master.Host, conf.Master.Port, grpc.WithInsecure())
	client := master.NewMasterClient(conn)
	msg, err := client.GetAllMinions(context.Background(), &master.Query{})
	if err != nil {
		log.Error.Println(err)
		return
	}

	connPool.GRPCDisconnect(conn)

	for i, host := range msg.Host {
		parts := strings.Split(host, ":")
		ip := parts[0]
		port := parts[1]

		minions[ip] = port
		minionsTime[ip] = msg.Time[i]
	}

	ctx := HTMLContext{
		"Minions":     minions,
		"MinionsTime": minionsTime,
	}

	render(w, ctx, "index.html")
}

func backupStart(w http.ResponseWriter, r *http.Request) {
	log.Info.Println(r.URL.Path)

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

	log.Info.Printf("Start backup %s, %s, %d", params["table"], params["host"], port)
	conn := connPool.GRPCConnect(r.Context(), params["host"], port, grpc.WithInsecure())
	client := minion.NewMinionClient(conn)
	msg, err := client.StartBackup(context.Background(), &minion.QueryStartBackup{Db: params["db"], Namespace: params["namespace"], Table: params["table"]})
	if err != nil {
		log.Error.Println(err)
		return
	}
	connPool.GRPCDisconnect(conn)

	config.Cache.Set(`alerts`, msg.Msg, 1*time.Minute)

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

	if r.Method != http.MethodPost {
		redirectUrl := fmt.Sprintf("/progress?db=%s&&host=%s&&port=%d", params["db"], params["host"], port)
		http.Redirect(w, r, redirectUrl, 302)
	}

	forms, _ := escapeForms(r, "schedule-time", "schedule-daily", "schedule-weekly", "schedule-monthly")

	conn := connPool.GRPCConnect(r.Context(), params["host"], port, grpc.WithInsecure())
	client := minion.NewMinionClient(conn)
	msg, err := client.ScheduleBackup(
		context.Background(),
		&minion.QueryScheduleBackup{
			Db:        params["db"],
			Namespace: params["namespace"],
			Table:     params["table"],
			Timestamp: forms["schedule-time"],
		},
	)

	if err != nil {
		log.Error.Println(err)
		return
	}
	connPool.GRPCDisconnect(conn)

	config.Cache.Set(`alerts`, msg.Msg, 1*time.Minute)

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

	conn := connPool.GRPCConnect(r.Context(), params[`host`], port, grpc.WithInsecure())
	client := minion.NewMinionClient(conn)

	_tablesFromCache, ok := config.Cache.Get(params[`db`] + `:tables`)
	if !ok {
		tables, err := client.GetTables(context.Background(), &minion.QueryGetTables{Db: params[`db`]})
		if err != nil {
			log.Error.Println(err)
			return
		}

		var _tmpTables []map[string]string
		for _, tb := range tables.Tables {
			if strings.EqualFold(tb, "TABLE") ||
				strings.EqualFold(tb, "147") ||
				strings.EqualFold(tb, "") ||
				strings.EqualFold(tb, "HBase") ||
				strings.EqualFold(tb, "Type") ||
				strings.EqualFold(tb, "Version") {
				continue
			}

			parts := strings.Split(tb, ":")

			var (
				namespace string
				tablename string
			)

			if len(parts) > 1 {
				namespace = parts[0]
				tablename = parts[1]
			} else {
				namespace = "none"
				tablename = parts[0]
			}

			_tmpTables = append(
				_tmpTables,
				map[string]string{
					`namespace`: namespace,
					`tablename`: tablename,
				},
			)
		}

		config.Cache.Set(params[`db`]+`:tables`, _tmpTables, 15*time.Minute)
		_tablesFromCache = _tmpTables
	}
	tables = _tablesFromCache.([]map[string]string)

	connPool.GRPCDisconnect(conn)

	_Alerts, ok := config.Cache.Get(`alerts`)
	if _Alerts != nil {
		alerts = append(alerts, _Alerts.(string))
	}

	ctx := HTMLContext{
		"Db":     params["db"],
		"Time":   minionsTime[params["host"]],
		"Host":   params["host"],
		"Port":   params["port"],
		"Alerts": alerts,
		"Tables": tables,
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
