package hbase

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/colinmarc/hdfs"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/enabokov/backuper/internal/log"
)

type SourceFile struct {
	HostNameNode string
	Filename     string
}

type TargetS3 struct {
	Region     string
	BucketName string
	Key        string
}

func checkIsDir(client *hdfs.Client, filename string) bool {
	stat, err := client.Stat(filename)
	if err != nil {
		log.Error.Println(err)
	}

	if !stat.IsDir() {
		return false
	}

	return true
}

func _copyToS3Bucket(client *hdfs.Client, sess *session.Session, dirname string, dst TargetS3, wg *sync.WaitGroup) {
	defer wg.Done()
	bases, err := client.ReadDir(dirname)
	if err != nil {
		log.Error.Println(err)
		return
	}

	for _, base := range bases {
		log.Info.Println(base.Name())
		fullPath := filepath.Join(dirname, base.Name())
		if checkIsDir(client, fullPath) {
			log.Warn.Println("It's not dir:", fullPath, "\nCopy file to", dst.BucketName)
			wg.Add(1)
			go _copyToS3Bucket(client, sess, fullPath, dst, wg)
			continue
		}

		UploadFileToS3Bucket(sess, client, fullPath, dst)
	}
}

func close(client *hdfs.Client) {
	err := client.Close()
	log.Error.Println(err)
}

func copyDirToS3Bucket(src SourceFile, dst TargetS3) {
	hdfsclient := getHDFSClient(src)
	if hdfsclient == nil {
		log.Error.Println("Failed to create HDFS client")
		return
	}
	defer close(hdfsclient)

	sess := getAWSClient(dst)
	if sess == nil {
		log.Error.Println("Failed to create AWS client")
		return
	}

	if !checkIsDir(hdfsclient, src.Filename) {
		log.Warn.Println("It's not dir:", src.Filename, "\nCopy file to", dst.BucketName)
		UploadFileToS3Bucket(sess, hdfsclient, src.Filename, dst)
		return
	}

	log.Info.Printf("Start copying dir %s -> %s\n", src.Filename, dst.BucketName)

	// each goroutine backups one dir
	var wg sync.WaitGroup

	wg.Add(1)
	go _copyToS3Bucket(hdfsclient, sess, src.Filename, dst, &wg)
	wg.Wait()
}

func CopyToS3Bucket(src SourceFile, dst TargetS3) {
	copyDirToS3Bucket(src, dst)
}

func getNamespaces(src SourceFile) (names []string, sizes []string) {
	hdfsclient := getHDFSClient(src)
	if hdfsclient == nil {
		log.Error.Println("Failed to create HDFS client")
		return nil, nil
	}
	defer close(hdfsclient)

	stat, err := hdfsclient.Stat(src.Filename)
	if err != nil {
		log.Error.Println(err)
	}

	if !stat.IsDir() {
		log.Warn.Println("It's not dir:", src.Filename)
		return nil, nil
	}

	namespaces, err := hdfsclient.ReadDir(src.Filename)

	names = make([]string, len(namespaces))
	sizes = make([]string, len(namespaces))

	for i, namespace := range namespaces {
		names[i] = namespace.Name()
		stat, err := hdfsclient.GetContentSummary(filepath.Join(src.Filename, namespace.Name()))
		if err != nil {
			log.Error.Println(err)
		}

		sizes[i] = strconv.Itoa(int(stat.Size()))
	}

	return names, sizes
}

func GetNamespaces(src SourceFile) ([]string, []string) {
	return getNamespaces(src)
}
