package main

type Storage interface {
	MergeClient(string, *Connection) (*Connection, error)
	DeleteClient(string, *Connection) error
	GetTopicClientList(string) []string
	GetClientConnection(topic string) *Connection
	Subscribe(topic string, clientid string)
}
