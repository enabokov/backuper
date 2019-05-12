package hbase

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/colinmarc/hdfs"
	"path/filepath"

	"github.com/enabokov/backuper/internal/log"
)

func getAWSClient(dst TargetS3) *session.Session {
	s, err := session.NewSession(&aws.Config{Region: aws.String(dst.Region)})
	if err != nil {
		log.Error.Println(err)
		return nil
	}

	return s
}

func UploadFileToS3Bucket(sess *session.Session, c *hdfs.Client, filename string, conf TargetS3) {
	file, err := c.Open(filename)
	if err != nil {
		log.Error.Println("Failed to open src file")
		log.Error.Println(err)
	}
	defer file.Close()

	key := filepath.Join(conf.Key, filename)
	log.Info.Printf("Start backup %s -> s3://%s", filename, key)

	objInput := &s3.PutObjectInput{
		Bucket:               aws.String(conf.BucketName),
		Key:                  aws.String(key),
		ACL:                  aws.String("private"),
		Body:                 file,
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	}

	if _, err = s3.New(sess).PutObject(objInput); err != nil {
		log.Error.Println(err)
	}

	log.Info.Printf("Finish backup %s -> s3://%s", filename, key)
}
