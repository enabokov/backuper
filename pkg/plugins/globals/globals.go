package globals

import (
	"fmt"
)

type Socket struct {
	IP   string
	Port string
}

func (s *Socket) GetHost() string {
	return fmt.Sprintf("%s:%s", s.IP, s.Port)
}

type S3Options struct {
	Region     string
	BucketName string
	Key        string
}
