package web

import (
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path/filepath"

	"github.com/enabokov/backuper/internal/log"
)

func escapeParams(r *http.Request, params ...string) (map[string]string, error) {
	dict := make(map[string]string)

	for _, param := range params {
		p, ok := r.URL.Query()[param]
		if !ok || len(p[0]) < 1 {
			log.Error.Printf("Url param %s is missing\n", param)
			return nil, fmt.Errorf("param %s is missing", param)
		}

		dict[param] = p[0]
	}

	return dict, nil
}

func render(w http.ResponseWriter, ctx interface{}, page string) {
	wd, err := os.Getwd()
	if err != nil {
		log.Error.Println(err)
	}

	tmpl := template.Must(
		template.ParseFiles(
			append([]string{wd + "/web/template/base.html"}, filepath.Join(wd+"/web/template/", page))...))

	if err := tmpl.Execute(w, ctx); err != nil {
		log.Error.Fatalln(err)
	}
}
