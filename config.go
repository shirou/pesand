package main

import (
	"code.google.com/p/gcfg"
)

type Config struct {
	Default struct {
		DebugLevel string
		Storage    string
		Port       string
	}
	Queue struct {
		SendingQueueLength int
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
