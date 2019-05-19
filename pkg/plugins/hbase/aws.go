package hbase

import (
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/colinmarc/hdfs"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/enabokov/backuper/internal/log"
	"github.com/enabokov/backuper/pkg/plugins/globals"
)

func getAWSClient(options globals.S3Options) *session.Session {
	awsCfg := &aws.Config{
		Region: aws.String(options.Region),
	}

	s, err := session.NewSession(awsCfg)
	if err != nil {
		log.Error.Println(err)
		return nil
	}

	return s
}

func calcRemoteChecksum(sess *session.Session, bucket string, key string) string {
	objGet := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	s3obj, err := s3.New(sess).GetObject(objGet)
	if err != nil {
		log.Error.Println(err)
		return ""
	}

	return strings.Trim(*(s3obj.ETag), "\"")
}

func calcLocalChecksum(client *hdfs.Client, filename string) string {
	contents, err := client.ReadFile(filename)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%x", md5.Sum(contents))
}

func uploadSnapshotToS3(snapshotname *string, options *globals.S3Options) bool {
	var t = time.Now()

	key := filepath.Join(options.BucketName, options.Key, t.Format("2006-01-02"), *snapshotname)
	log.Info.Printf("Start uploading %s to S3 %s\n", *snapshotname, key)
	_, err := exec.Command(
		"hbase",
		"org.apache.hadoop.hbase.snapshot.ExportSnapshot",
		fmt.Sprintf("-snapshot %s", *snapshotname),
		fmt.Sprintf("-copy-to s3a://%s", key)).Output()
	if err != nil {
		log.Error.Println(err)
		return false
	}

	log.Info.Printf("Finish uploading %s to S3 %s\n", snapshotname, key)
	return true
}

// TODO: in progress: finish download from S3 bucket objects
//func uploadFileFromS3(sess *session.Session, c *hdfs.Client, filename string, conf globals.S3Options) bool {
//	file, err := c.Open(filename)
//	if err != nil {
//		log.Error.Println("Failed to open src file")
//		log.Error.Println(err)
//	}
//	defer file.Close()
//
//	key := filepath.Join(conf.Key, filename)
//	log.Info.Printf("Start backup %s -> s3://%s", filename, key)
//
//	objPutInput := &s3.PutObjectInput{
//		Bucket:               aws.String(conf.BucketName),
//		Key:                  aws.String(key),
//		Body:                 file,
//	}
//
//	// if failed, use multi part upload
//	if _, err := s3.New(sess).PutObject(objPutInput); err != nil {
//		log.Error.Println(err, filename)
//
//		objUploadInput := &s3manager.UploadInput{
//			Bucket:               aws.String(conf.BucketName),
//			Key:                  aws.String(key),
//			Body:                 file,
//		}
//
//		if _, err = s3manager.NewUploader(sess).Upload(objUploadInput); err != nil {
//			log.Error.Println(err, filename)
//		}
//
//		return false
//	}
//
//	log.Info.Printf("Finish backup %s -> s3://%s", filename, key)
//
//	checksumLocal := calcLocalChecksum(c, filename)
//	checksumRemote := calcRemoteChecksum(sess, conf.BucketName, key)
//	return checksumRemote == checksumLocal
//}
