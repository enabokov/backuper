package rpc

type Minion struct{}

type Database interface {
	GetNamespaces(socket interface{}) (names, sizes []string, checksums []float64)

	CreateSnapshot(namespace, tablename string) string
	CreateTableFromSnapshot(snapshot string) (string, error)

	CopyToS3Bucket(socket interface{}, src string, s3 interface{})

	DeleteSnapshot(namespace, tablename, timestamp string) string
	ListSnapshots() []string
}

type StrategyCli struct {
	strategy Database
}

func (s *StrategyCli) setDatabase(cli Database) {
	s.strategy = cli
}

func (s *StrategyCli) getDatabase() Database {
	return s.strategy
}
