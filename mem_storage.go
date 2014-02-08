package main

import (
	proto "github.com/huin/mqtt"
	"log"
	"sync"
)

type MemStorage struct {
	clientsMu  sync.Mutex
	clients    map[string]*Connection // clientid -> list of Connection
	TopicTable map[string][]string    // topic -> list of clientid
	RetainMap  map[string]*proto.Publish
}

func (mem *MemStorage) MergeClient(clientid string, conn *Connection) (*Connection, error) {
	mem.clientsMu.Lock()
	defer mem.clientsMu.Unlock()

	if _, ok := mem.clients[clientid]; ok {
		c := mem.clients[clientid]
		if c.Status == ClientAvailable {
			log.Println("Re-con")
		}

		return c, nil
	}
	mem.clients[clientid] = conn

	return conn, nil
}

func (mem *MemStorage) DeleteClient(clientid string, conn *Connection) error {
	return nil
}

func (mem *MemStorage) AddMsg(m *proto.Publish) {

}

func (mem *MemStorage) DeleteMsg(m *proto.Publish) {

}

func (mem *MemStorage) Flush() {

}

func (mem *MemStorage) GetTopicClientList(topic string) []string {
	return mem.TopicTable[topic]
}

func (mem *MemStorage) GetClientConnection(clientid string) *Connection {
	return mem.clients[clientid]
}

func (mem *MemStorage) Subscribe(topic string, clientid string) {
	mem.TopicTable[topic] = append(mem.TopicTable[topic], clientid)
}

func (mem *MemStorage) Unsubscribe(topic string, clientid string) {
	// TODO
}

func (mem *MemStorage) UpdateRetain(topic string, m *proto.Publish) {
	//does not need lock or check exists. just update it
	mem.RetainMap[topic] = m
}
func (mem *MemStorage) GetRetain(topic string) (*proto.Publish, bool) {
	m, ok := mem.RetainMap[topic]
	return m, ok
}

func NewMemStorage() *MemStorage {
	s := &MemStorage{
		clients:    make(map[string]*Connection),
		TopicTable: make(map[string][]string),
		RetainMap:  make(map[string]*proto.Publish),
	}
	return s
}
