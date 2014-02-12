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
	Unsubscribe(topic string, clientid string)
	UpdateRetain(topic string, m *proto.Publish)
	GetRetain(topic string) (*proto.Publish, bool)
	DeleteMsg(storedMsgId string) (err error)
	StoreMsg(clientid string, m *proto.Publish) (storedMsgId string)
}
