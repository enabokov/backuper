package web

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"net/http"
	"strconv"
	"strings"

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

func init() {
	c = config.InjectStorage
	connPool = pool.GetPool()
	minions = make(map[string]string)
}

// TODO: tmp cache -> change to real cache
var cacheNamespaces []map[string]string
var cacheAlerts []string

// tmp cache

func getIndexPage(w http.ResponseWriter, r *http.Request) {
	log.Info.Println(r.URL.Path)

	conf := c.GetDashboardConf()
	conn := connPool.GRPCConnect(r.Context(), conf.Master.Host, conf.Master.Port, grpc.WithInsecure())
	client := master.NewMasterClient(conn)
	msg, err := client.GetAllMinions(context.Background(), &master.Query{Query: "get all minions"})
	if err != nil {
		log.Error.Println(err)
		return
	}

	connPool.GRPCDisconnect(conn)

	for _, host := range msg.Host {
		_tmp := strings.Split(host, ":")
		ip := _tmp[0]
		port := _tmp[1]

		minions[ip] = port
	}
	ctx := HTMLContext{
		"Minions": minions,
	}

	render(w, ctx, "index.html")
}

func backupStart(w http.ResponseWriter, r *http.Request) {
	log.Info.Println(r.URL.Path)

	params, err := escapeParams(r, "host", "port", "db", "path")
	if err != nil {
		log.Error.Println(err)
		http.Redirect(w, r, "/400", http.StatusFound)
		return
	}

	port, err := strconv.Atoi(params["port"])
	if err != nil {
		log.Error.Println(err)
	}

	log.Info.Println("Start backup", params["host"], port)

	conn := connPool.GRPCConnect(r.Context(), params["host"], port, grpc.WithInsecure())
	client := minion.NewMinionClient(conn)
	msg, err := client.StartBackup(context.Background(), &minion.Query{Query: params["path"]})
	if err != nil {
		log.Error.Println(err)
		return
	}

	connPool.GRPCDisconnect(conn)

	cacheAlerts = append(cacheAlerts, msg.Msg)
	log.Info.Println(msg)
	ctx := HTMLContext{
		"Minions":    minions,
		"Alerts":     []string{msg.Msg},
		"Db":         params["db"],
		"Host":       params["host"],
		"Port":       params["port"],
		"Namespaces": cacheNamespaces,
	}

	redirectUrl := fmt.Sprintf("/progress?db=%s&&host=%s&&port=%s", params["db"], params["host"], params["port"])
	http.Redirect(w, r, redirectUrl, 302)
	render(w, ctx, "progress.html")
}

func backupProgress(w http.ResponseWriter, r *http.Request) {
	log.Info.Println(r.URL.Path)
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
	namespaces, err := client.GetNamespaces(context.Background(), &minion.Query{Query: `/hbase/data`})
	if err != nil {
		log.Error.Println(err)
		return
	}
	connPool.GRPCDisconnect(conn)

	// TODO : tmp cache -> change to real cache
	cacheNamespaces = nil
	for i, ns := range namespaces.Names {
		_tmp := make(map[string]string)
		_tmp[`name`] = ns
		_tmp[`size`] = namespaces.Sizes[i]

		cacheNamespaces = append(cacheNamespaces, _tmp)
	}

	log.Info.Println(params["host"])
	ctx := HTMLContext{
		"Db":         params["db"],
		"Host":       params["host"],
		"Port":       params["port"],
		"Alerts":     cacheAlerts,
		"Namespaces": cacheNamespaces,
	}

	// TODO: tmp cache -> change to real cache
	// clear messages
	cacheAlerts = nil
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
	http.HandleFunc("/progress", backupProgress)

	// error handlers
	http.HandleFunc("/400", errorBadRequest)
	//http.HandleFunc("/401", error401)
	//http.HandleFunc("/403", error403)
	http.HandleFunc("/404", errorNotFound)
}
