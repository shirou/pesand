package main

import (
	"code.google.com/p/gcfg"
	"time"
)

type Config struct {
	Default struct {
		DebugLevel       string
		Storage          string
		Port             string
		StatsIntervalSec time.Duration
	}
	Auth struct {
		JWTKey   string //for jwt auth
		Username string //for basic auth
		Password string //for basic auth
	}
	Queue struct {
		SendingQueueLength int
		SentQueueLength    int
	}
	LevelDB struct {
		DBFile string
	}
}

func NewConfig(config_path string) (Config, error) {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, config_path)

	return cfg, err
}
