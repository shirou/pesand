package main

import (
	proto "github.com/huin/mqtt"
)

type Storage interface {
	MergeClient(string, *Connection) (*Connection, error)
	DeleteClient(string, *Connection) error
	GetTopicClientList(string) []string
	GetClientConnection(topic string) *Connection
	Subscribe(topic string, clientid string)
	UpdateRetain(topic string, m *proto.Publish)
	GetRetain(topic string) (*proto.Publish, bool)
}
