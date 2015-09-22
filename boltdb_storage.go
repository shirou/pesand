package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	proto "github.com/huin/mqtt"
	"time"
)

type BoltStorage struct {
	DB     *bolt.DB
	DBFile string

	// clientid -> list of Session
	ClientBucket string
	// storeid -> StoredMsg
	StoredMsgBucket string
	// topic -> list of clientid
	TopicBucket string
	//RetainMap  map[string]*proto.Publish
	RetainBucket string
}

//Conn
func (b *BoltStorage) Conn() {

	db, err := bolt.Open(b.DBFile, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		log.Fatalf("bolt db open error: %v", err)
	}
	b.DB = db
}

//Close
func (b *BoltStorage) Close() {
	err := b.DB.Close()
	if err != nil {
		log.Fatal(err)
	}
}

//Setup
func (b *BoltStorage) Setup() {

	b.DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(b.ClientBucket))
		if err != nil {
			return fmt.Errorf("create client bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(b.StoredMsgBucket))
		if err != nil {
			return fmt.Errorf("create stored message bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(b.TopicBucket))
		if err != nil {
			return fmt.Errorf("create topic bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(b.RetainBucket))
		if err != nil {
			return fmt.Errorf("create retain bucket: %s", err)
		}
		return nil
	})
}

func (c *Session) gobEncode() ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(c)
	if err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func (c *Session) gobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	err := decoder.Decode(&c)
	if err != nil {
		return err
	}

	return nil
}

//set
func (b *BoltStorage) set(bucketName string, key string, val []byte) error {

	err := b.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		return bucket.Put([]byte(key), val)
	})
	return err

}

//get
func (b *BoltStorage) get(bucketName, key string) (val []byte, err error) {

	err = b.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		val = bucket.Get([]byte(key))
		return nil
	})
	return
}

//del
func (b *BoltStorage) del(bucketName, key string) error {
	err := b.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		return bucket.Delete([]byte(key))
	})
	return err
}

//MergeClient
func (b *BoltStorage) MergeClientSession(clientid string, sess *Session, clean int) (*Session, error) {

	clientLog := log.WithField("clientID", clientid)

	s, err := b.GetClientSession(clientid)
	if err != nil {
		return nil, err
	}
	log.Println(s)
	if s != nil {
		// clean flag is true, clean it
		if clean == 0 {
			clientLog.Debug("clean flag is true, delete old client")
			b.DeleteClientSession(clientid)

		} else { // clean flag is false, reuse existsted clients

			if s.Status == ClientAvailable {
				clientLog.Debug("client has been reconnected")
				return s, nil
			}
		}
	}
	log.Printf("setclientsession %s : %v", clientid, sess)

	err = b.SetClientSession(clientid, sess)
	if err != nil {
		return nil, err
	}

	return sess, nil
}

//DeleteClient
func (b *BoltStorage) DeleteClientSession(clientid string) error {
	return b.del(b.ClientBucket, clientid)
}

//GetClientSesion
func (b *BoltStorage) GetClientSession(clientid string) (*Session, error) {

	val, err := b.get(b.ClientBucket, clientid)
	if err != nil {
		return nil, err
	}

	if len(val) == 0 {
		return nil, nil
	}
	clientSess := &Session{}
	err = clientSess.gobDecode(val)
	if err != nil {
		return nil, err
	}

	return clientSess, nil
}

//SetClientConnection
func (b *BoltStorage) SetClientSession(clientid string, sess *Session) error {

	data, err := sess.gobEncode()
	if err != nil {
		return fmt.Errorf("gob encode failed:", err)
	}

	return b.set(b.ClientBucket, clientid, data)

}

//StoreMsg
func (b *BoltStorage) StoreMsg(clientid string, m *proto.Publish) (string, error) {

	storedMsgId := createStoredMsgId(clientid, m)
	s := &StoredMsg{
		lastupdated: time.Now(),
		clientid:    clientid,
		Message:     m,
		status:      StoredMsgSending,
	}

	w := new(bytes.Buffer)
	gob.Register(&proto.Publish{})
	gob.Register(&proto.BytesPayload{})
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(s)
	if err != nil {
		return "", err
	}

	err = b.set(b.StoredMsgBucket, clientid, w.Bytes())
	if err != nil {
		return "", err
	}

	return storedMsgId, nil

}

//DeleteMsg
func (b *BoltStorage) DeleteMsg(storedMsgId string) (err error) {

	return b.del(b.StoredMsgBucket, storedMsgId)

}

//GetTopicClientList
func (b *BoltStorage) GetTopicClientList(topic string) ([]string, error) {

	var topicClientList []string
	//topicClientList := make([]string, 10)

	err := b.DB.View(func(tx *bolt.Tx) error {
		//get topicBucket
		bucket := tx.Bucket([]byte(b.TopicBucket))
		//get nested individual topic bucket
		topicBucket := bucket.Bucket([]byte(topic))
		if topicBucket == nil {
			return nil //return fmt.Errorf("topic bucket doesn't exist: %s", topic)
		}
		//iterate over bucket keys add to string list
		topicBucket.ForEach(func(k, v []byte) error {
			topicClientList = append(topicClientList, string(v))
			return nil
		})
		return nil
	})

	return topicClientList, err

}

//Subscribe
func (b *BoltStorage) Subscribe(topic string, clientid string) error {

	//Use a nested bucket
	err := b.DB.Update(func(tx *bolt.Tx) error {
		//get topicBucket
		bucket := tx.Bucket([]byte(b.TopicBucket))
		//create nested bucket for each topic
		topicBucket, err := bucket.CreateBucketIfNotExists([]byte(topic))
		if err != nil {
			return fmt.Errorf("error creating nested topic bucket: %s", err)
		}
		//fill nested topic bucket with client id
		return topicBucket.Put([]byte(clientid), []byte(clientid))
	})

	return err

}

//Unsubscribe
func (b *BoltStorage) Unsubscribe(topic string, clientid string) error {

	err := b.DB.Update(func(tx *bolt.Tx) error {
		//get topicBucket
		bucket := tx.Bucket([]byte(b.TopicBucket))
		//get nested individual topic bucket
		topicBucket := bucket.Bucket([]byte(topic))
		//delete clientid from nested bucket
		return topicBucket.Delete([]byte(clientid))
	})

	return err

}

//UpdateRetain
func (b *BoltStorage) UpdateRetain(topic string, m *proto.Publish) error {

	w := new(bytes.Buffer)
	gob.Register(&proto.BytesPayload{})
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(m)
	if err != nil {
		return err
	}
	return b.set(b.RetainBucket, topic, w.Bytes())

}

//GetRetain
func (b *BoltStorage) GetRetain(topic string) (*proto.Publish, error) {

	val, err := b.get(b.RetainBucket, topic)
	if err != nil {
		return nil, err
	}

	if len(val) == 0 {
		return nil, fmt.Errorf("topic %s contains no retain messages", topic)
	}

	//from []bytes to *proto.Publish
	msg := &proto.Publish{}
	r := bytes.NewBuffer(val)
	decoder := gob.NewDecoder(r)
	err = decoder.Decode(&msg)
	if err != nil {
		return nil, err
	}

	//binary.Read(bytes.NewBuffer(val), binary.LittleEndian, &msg)

	return msg, nil

}

func NewBoltStorage(dbfile string) *BoltStorage {
	s := &BoltStorage{
		ClientBucket:    "mqttClients",
		TopicBucket:     "mqttTopics",
		RetainBucket:    "mqttRetain",
		StoredMsgBucket: "mqttStoredMsg",
		DBFile:          dbfile,
	}
	s.Conn()
	s.Setup()
	return s
}
