package main

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	proto "github.com/huin/mqtt"
	"sync"
	"time"
)

type MemStorage struct {
	clientsMu sync.Mutex
	// clientid -> list of Session
	clients map[string]*Session
	// topic -> list of clientid
	TopicTable map[string][]string
	RetainMap  map[string]*proto.Publish
	// storeid -> StoredMsg
	StoredMessages map[string]*StoredMsg
}

//MergeClient
func (mem *MemStorage) MergeClientSession(clientid string, sess *Session, clean int) (*Session, error) {
	mem.clientsMu.Lock()
	defer mem.clientsMu.Unlock()

	clientLog := log.WithField("clientID", clientid)

	if _, ok := mem.clients[clientid]; ok {
		// clean flag is true, clean it
		if clean == 0 {
			clientLog.Debug("clean flag is true, delete old client")

			mem.DeleteClientSession(clientid)
		} else {
			// clean flag is false, reuse existsted clients
			c := mem.clients[clientid]
			if c.Status == ClientAvailable {
				clientLog.Debug("client has been reconnected")

				return c, nil
			}
		}
	}

	mem.clients[clientid] = sess

	return sess, nil
}

//DeleteClient
func (mem *MemStorage) DeleteClientSession(clientid string) error {
	return nil
}

//StoreMsg
func (mem *MemStorage) StoreMsg(clientid string, m *proto.Publish) (storedMsgId string, err error) {
	storedMsgId = createStoredMsgId(clientid, m)

	s := &StoredMsg{
		lastupdated: time.Now(),
		clientid:    clientid,
		Message:     m,
		status:      StoredMsgSending,
	}
	mem.StoredMessages[storedMsgId] = s

	return storedMsgId, nil
}

//DeleteMsg
func (mem *MemStorage) DeleteMsg(storedMsgId string) (err error) {
	if _, ok := mem.clients[storedMsgId]; ok {
		delete(mem.StoredMessages, storedMsgId)
		return nil
	} else {
		return errors.New(storedMsgId + " is not exists")
	}
}

//Flush
func (mem *MemStorage) Flush() {

}

//GetTopicClientList
func (mem *MemStorage) GetTopicClientList(topic string) ([]string, error) {
	return mem.TopicTable[topic], nil
}

//GetClientSession
func (mem *MemStorage) GetClientSession(clientid string) (*Session, error) {
	return mem.clients[clientid], nil
}

//SetClientSession
func (mem *MemStorage) SetClientSession(clientid string, sess *Session) error {
	mem.clients[clientid] = sess
	return nil
}

//Subscribe
func (mem *MemStorage) Subscribe(topic string, clientid string) error {
	mem.TopicTable[topic] = append(mem.TopicTable[topic], clientid)
	return nil
}

//Unsubscribe
func (mem *MemStorage) Unsubscribe(topic string, clientid string) error {
	a := mem.TopicTable[topic]
	for i, cid := range a {
		if clientid == cid {
			mem.TopicTable[topic] = append(a[:i], a[i+1:]...)
		}
	}
	return nil
}

//UpdateRetain
func (mem *MemStorage) UpdateRetain(topic string, m *proto.Publish) error {
	//does not need lock or check exists. just update it
	mem.RetainMap[topic] = m
	return nil
}

//GetRetain
func (mem *MemStorage) GetRetain(topic string) (*proto.Publish, error) {
	m, ok := mem.RetainMap[topic]
	if !ok {
		return m, fmt.Errorf("Error getting retain topic")
	}

	return m, nil
}

func NewMemStorage() *MemStorage {
	s := &MemStorage{
		clients:        make(map[string]*Session),
		TopicTable:     make(map[string][]string),
		RetainMap:      make(map[string]*proto.Publish),
		StoredMessages: make(map[string]*StoredMsg),
	}
	return s
}
