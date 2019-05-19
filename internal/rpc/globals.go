package rpc

type Minion struct{}

type Database interface {
	GetNamespaces(socket interface{}) (names, sizes []string, checksums []float64)
	GetTables(socket interface{}) []string

	BackupTableToS3(socket interface{}, namespace string, tablename string, s3 interface{})

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
