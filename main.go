package main

import (
	"flag"
	"log"
	"net"
	"os"
)

func main() {
	var config_path string

	flag.StringVar(&config_path, "c", "", "config file")
	flag.Parse()

	// Load config file
	conf, err := NewConfig(config_path)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	port := conf.Default.Port
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	b := NewBroker(conf, listen)
	b.Start()
	<-b.Done
}
