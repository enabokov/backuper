package rpc

type Minion struct{}

type TableUnit struct {
	Name       string
	LastBackup string
	Namespace  string
}

type Database interface {
	GetNamespaces(socket interface{}) (names, sizes []string, checksums []float64)
	GetTables(socket interface{}) []string

	BackupSchedule(socket interface{}, namespace, tablename, timestamp string, s3 interface{})
	BackupInstant(socket interface{}, namespace, tablename string, s3 interface{})

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
