package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	log "github.com/Sirupsen/logrus"
	proto "github.com/huin/mqtt"
	"hash/fnv"
	"time"
)

type Storage interface {
	MergeClientSession(string, *Session, int) (sess *Session, err error)
	DeleteClientSession(string) error
	GetClientSession(clientid string) (*Session, error)
	SetClientSession(clientid string, sess *Session) error

	GetTopicClientList(topic string) ([]string, error)
	Subscribe(topic string, clientid string) error
	Unsubscribe(topic string, clientid string) error

	UpdateRetain(topic string, m *proto.Publish) error
	GetRetain(topic string) (*proto.Publish, error)

	DeleteMsg(storedMsgId string) (err error)
	StoreMsg(clientid string, m *proto.Publish) (storedMsgId string, err error)
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

func (c *Config) ConfigStorage() Storage {

	if c.Default.Storage == "rethinkdb" && c.RethinkDB.Url != "" {
		log.WithField("type", "RethinkDB").Info("Storage")
		return Storage(NewRethinkStorage(c.RethinkDB.Url, c.RethinkDB.Port, c.RethinkDB.DbName, c.RethinkDB.AuthKey))
	}

	if c.Default.Storage == "boltdb" && c.BoltDB.DBFile != "" {
		log.WithField("type", "BoltDB").Info("Storage")
		return Storage(NewBoltStorage(c.BoltDB.DBFile))
	}

	log.WithField("type", "Memory").Warn("Storage")
	return Storage(NewMemStorage())
}

// createStoredMsgId creates a uniq stored id from
// publish msg. This is used as a key of StoredMessages.
// <clientdi>-<msgid>-<randint>
// Note: QoS0 does not have a messageid, but it is not required to store.
// so this func is not invoked.
func createStoredMsgId(clientid string, m *proto.Publish) string {
	var randomness int32
	binary.Read(rand.Reader, binary.LittleEndian, &randomness) //add a little randomness
	inputString := fmt.Sprintf("%s-%v-%v", clientid, m.MessageId, randomness)
	h := fnv.New32a()
	h.Write([]byte(inputString))
	return fmt.Sprintf("%d", h.Sum32())
}
