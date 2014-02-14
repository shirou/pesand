package main

import (
	"errors"
	"fmt"
	proto "github.com/huin/mqtt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type MemStorage struct {
	clientsMu sync.Mutex
	// clientid -> list of Connection
	clients map[string]*Connection
	// topic -> list of clientid
	TopicTable map[string][]string
	RetainMap  map[string]*proto.Publish
	// storeid -> StoredMsg
	StoredMessages map[string]*StoredMsg
}

const (
	StoredMsgSent uint8 = iota
	StoredMsgSending
	StoredMsgWillBeDeleted
)

// A message which will be
type StoredMsg struct {
	lastupdated time.Time
	clientid    string
	Message     proto.Message
	status      uint8
}

func (mem *MemStorage) MergeClient(clientid string, conn *Connection, clean int) (*Connection, error) {
	mem.clientsMu.Lock()
	defer mem.clientsMu.Unlock()

	if _, ok := mem.clients[clientid]; ok {

		// clean flag is true, clean it
		if clean == 0 {
			mem.DeleteClient(clientid, conn)
		} else {
			// clean flag is false, reuse existsted clients
			c := mem.clients[clientid]
			if c.Status == ClientAvailable {
				log.Printf("client id %s has been reconnected.")
				return c, nil
			}
		}
	}
	mem.clients[clientid] = conn

	return conn, nil
}

func (mem *MemStorage) DeleteClient(clientid string, conn *Connection) error {
	return nil
}

// createStoredMsgId creates a uniq stored id from
// publish msg. This is used as a key of StoredMessages.
// <clientdi>-<msgid>-<randint>
// Note: QoS0 does not have a messageid, but it is not required to store.
// so this func is not invoked.
func createStoredMsgId(clientid string, m *proto.Publish) string {
	r := rand.Int()

	return fmt.Sprintf("%s-%v-%v", clientid, m.MessageId, r)
}

func (mem *MemStorage) StoreMsg(clientid string, m *proto.Publish) (storedMsgId string) {
	storedMsgId = createStoredMsgId(clientid, m)

	s := &StoredMsg{
		lastupdated: time.Now(),
		clientid:    clientid,
		Message:     m,
		status:      StoredMsgSending,
	}
	mem.StoredMessages[storedMsgId] = s

	return storedMsgId
}

func (mem *MemStorage) DeleteMsg(storedMsgId string) (err error) {
	if _, ok := mem.clients[storedMsgId]; ok {
		delete(mem.StoredMessages, storedMsgId)
		return nil
	} else {
		return errors.New(storedMsgId + " is not exists")
	}
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
	a := mem.TopicTable[topic]
	for i, cid := range a {
		if clientid == cid {
			mem.TopicTable[topic] = append(a[:i], a[i+1:]...)
		}
	}
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
		clients:        make(map[string]*Connection),
		TopicTable:     make(map[string][]string),
		RetainMap:      make(map[string]*proto.Publish),
		StoredMessages: make(map[string]*StoredMsg),
	}
	return s
}
