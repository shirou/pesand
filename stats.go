package main

import (
	"log"
)

type Stats struct {
}

func (s *Stats) clientDisconnect(c *Connection) {
	log.Printf("ClientDisconnected: %s", c.clientid)
}

func NewStats() Stats {
	return Stats{}
}
