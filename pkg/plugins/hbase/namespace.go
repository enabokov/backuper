package hbase

import (
	"github.com/colinmarc/hdfs"
	"path/filepath"
	"strconv"

	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/pkg/plugins/globals"
)

const pathToNamespaces = `/hbase/data/`

func getNamespaces(socket globals.Socket) (names, sizes []string, checksums []float64) {
	hdfscli, err := hdfs.New(socket.GetHost())
	if err != nil {
		log.Error.Println(err)
		return nil, nil, nil
	}

	if hdfscli == nil {
		log.Error.Println("Failed to create HDFS client")
		return nil, nil, nil
	}
	defer hdfscli.Close()

	namespaces, err := hdfscli.ReadDir(pathToNamespaces)

	names = make([]string, len(namespaces))
	sizes = make([]string, len(namespaces))
	checksums = make([]float64, len(namespaces))

	for i, namespace := range namespaces {
		names[i] = namespace.Name()
		stat, err := hdfscli.GetContentSummary(filepath.Join(pathToNamespaces, namespace.Name()))
		if err != nil {
			log.Error.Println(err)
		}

		sizes[i] = strconv.Itoa(int(stat.Size()))

		// add checksums checker

	}

	return names, sizes, checksums
}
