package config

import (
	"github.com/patrickmn/go-cache"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"

	"github.com/enabokov/backuper/internal/log"
)

type _ConfMaster struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type _ConfS3Bucket struct {
	Name   string `yaml:"name"`
	Region string `yaml:"region"`
}

type _ConfS3 struct {
	Bucket _ConfS3Bucket `yaml:"bucket"`
}

type _ConfNameNode struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type _ConfTarget struct {
	S3 _ConfS3 `yaml:"s3"`

	// add targets here
	// ...
}

type ConfMinion struct {
	Port      int           `yaml:"port"`
	Heartbeat int64         `yaml:"heartbeat"`
	Master    _ConfMaster   `yaml:"master"`
	NameNode  _ConfNameNode `yaml:"namenode"`
	Targets   _ConfTarget   `yaml:"targets"`
}

type ConfMaster struct {
	Port int `yaml:"port"`
}

type ConfDashboard struct {
	Port   int         `yaml:"port"`
	Master _ConfMaster `yaml:"master"`
}

var InjectStorage Storage

type Storage struct {
	Configs map[string]interface{}
}

type IStorage interface {
	GetMasterConf() *ConfMaster
	GetMinionConf() *ConfMinion
	GetDashboardConf() *ConfDashboard
	Put(filename, injectName string, out interface{})
}

func (i *Storage) GetMasterConf() *ConfMaster {
	return i.Configs[`master`].(*ConfMaster)
}

func (i *Storage) GetMinionConf() *ConfMinion {
	return i.Configs[`minion`].(*ConfMinion)
}

func (i *Storage) GetDashboardConf() *ConfDashboard {
	return i.Configs[`dashboard`].(*ConfDashboard)
}

func (i *Storage) Put(filename, injectName string, out interface{}) {
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Error.Fatalln(err)
	}

	if err = yaml.Unmarshal(f, out); err != nil {
		log.Error.Fatalln(err)
	}

	i.Configs[injectName] = out
}

var Cache = cache.New(1*time.Minute, 1*time.Minute)

func init() {
	InjectStorage = Storage{}
	InjectStorage.Configs = make(map[string]interface{})
}
