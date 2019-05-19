package main

import (
	"fmt"
	"net/http"
	"runtime"

	"github.com/enabokov/backuper/internal/config"
	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/internal/web"
)

var c config.ConfDashboard

func init() {
	config.InjectStorage.Put("configs/dashboard.yaml", `dashboard`, &c)
}

func main() {
	runtime.GOMAXPROCS(1)

	web.SetupHandlers()
	log.Info.Println("Listening on ...", c.Port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", c.Port), nil); err != nil {
		log.Error.Fatalln(err)
	}
}
