package main

import (
	"bytes"
	"fmt"
	log "github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	proto "github.com/huin/mqtt"
	//"io"
	//"encoding/json"
	"time"
)

type RethinkStorage struct {
	Url     string
	Port    string
	DbName  string
	AuthKey string
	Session *r.Session

	// clientid -> list of Session
	ClientTable string
	// storeid -> StoredMsg
	StoredMsgTable string
	// topic -> list of clientid
	TopicTable string
	//RetainMap  map[string]*proto.Publish
	RetainTable string
}

type Retain struct {
	Topic string         `gorethink:"topic"`
	Msg   *proto.Publish `gorethink:"msg"`
}

type Topic struct {
	ID      string   `gorethink:"id"`
	Clients []string `gorethink:"clients"`
}

//Conn
func (rt *RethinkStorage) Conn() {

	sess, err := r.Connect(r.ConnectOpts{
		Addresses:     []string{rt.Url + ":" + rt.Port},
		Database:      rt.DbName,
		AuthKey:       rt.AuthKey,
		MaxOpen:       40,
		DiscoverHosts: true,
	})
	if err != nil {
		log.Fatal("Rethink-connectDB:", err)
	}

	r.SetVerbose(true)

	rt.Session = sess
}

//MergeClient
func (rt *RethinkStorage) MergeClientSession(clientid string, sess *Session, clean int) (*Session, error) {

	clientLog := log.WithField("clientID", clientid)

	// clean flag is true, clean it
	if clean == 0 {
		clientLog.Debug("clean flag is true, delete old client")
		rt.DeleteClientSession(clientid)

	} else { // clean flag is false, reuse existsted clients

		c, err := rt.GetClientSession(clientid)
		if err != nil {
			return nil, err
		}

		if c.Status == ClientAvailable {
			clientLog.Debug("client has been reconnected")
			return c, nil
		}
	}

	err := rt.SetClientSession(clientid, sess)
	if err != nil {
		return nil, err
	}

	return sess, nil
}

//DeleteClient
func (rt *RethinkStorage) DeleteClientSession(clientid string) error {

	// Delete the item
	response, err := r.Table(rt.ClientTable).Get(clientid).Delete().RunWrite(rt.Session)
	if err != nil {
		return fmt.Errorf("Error Deleting by ID: %s", err)
	}

	if response.Errors == 1 {
		return fmt.Errorf("Error deleting data: %s", response.FirstError)
	}

	return nil
}

//GetClientSession
func (rt *RethinkStorage) GetClientSession(clientid string) (*Session, error) {

	log.Printf("GetClientSession:%s", clientid)

	cursor, err := r.Table(rt.ClientTable).
		Get(clientid).
		Run(rt.Session)
	if err != nil {
		return nil, fmt.Errorf("Error Finding by ID: %s", err)
	}
	defer cursor.Close()

	if cursor.IsNil() {
		return nil, fmt.Errorf("Error Entry not found")
	}

	clientCon := &Session{}
	err = cursor.One(clientCon)
	if err == r.ErrEmptyResult {
		return nil, fmt.Errorf("Error:%s", r.ErrEmptyResult.Error()) // row not found
	}
	if err != nil {
		return nil, fmt.Errorf("Error scanning db result: %s", err)
	}

	return clientCon, nil
}

//SetClientSession
func (rt *RethinkStorage) SetClientSession(clientid string, sess *Session) error {

	response, err := r.Table(rt.ClientTable).
		Get(clientid).
		Update(sess).
		RunWrite(rt.Session)
	if err != nil {
		return fmt.Errorf("Error updating data: %s", err)
	}

	if response.Errors == 1 {
		return fmt.Errorf("Error updating data: %s", response.FirstError)
	}

	return nil

}

//StoreMsg
func (rt *RethinkStorage) StoreMsg(clientid string, m *proto.Publish) (storedMsgId string, err error) {

	storedMsgId = createStoredMsgId(clientid, m)
	s := &StoredMsg{
		lastupdated: time.Now(),
		clientid:    clientid,
		Message:     m,
		status:      StoredMsgSending,
	}

	response, err := r.Table(rt.StoredMsgTable).Insert(s).RunWrite(rt.Session)
	if err != nil {
		return "", fmt.Errorf("Error inserting data: %s", err.Error())
	}

	if response.Errors == 1 {
		return "", fmt.Errorf("Error inserting data: %s", response.FirstError)
	}

	return storedMsgId, nil
}

//DeleteMsg
func (rt *RethinkStorage) DeleteMsg(storedMsgId string) (err error) {

	// Delete the item
	response, err := r.Table(rt.StoredMsgTable).
		Get(storedMsgId).
		Delete().
		RunWrite(rt.Session)
	if err != nil {
		return fmt.Errorf("Error Deleting Stored Msg: %s", err)
	}

	if response.Errors == 1 {
		return fmt.Errorf("Error Deleting Stored Msg: %s", response.FirstError)
	}

	return nil

}

//GetTopicClientList
func (rt *RethinkStorage) GetTopicClientList(topic string) ([]string, error) {

	cursor, err := r.Table(rt.TopicTable).
		Get(topic).
		Run(rt.Session)
	if err != nil {
		return nil, fmt.Errorf("Error Finding by ID: %s", err)
	}
	defer cursor.Close()

	if cursor.IsNil() {
		return nil, fmt.Errorf("Error Entry not found")
	}

	var newTopic Topic
	err = cursor.One(&newTopic)
	if err == r.ErrEmptyResult {
		return nil, fmt.Errorf("Error:%s", r.ErrEmptyResult.Error()) // row not found
	}
	if err != nil {
		return nil, fmt.Errorf("Error scanning db result: %s", err)
	}

	return newTopic.Clients, nil
}

//Subscribe
func (rt *RethinkStorage) Subscribe(topic string, clientid string) error {

	/*
		var newDoc = { metaData: { ...}, results: [...] };

		r.db("foo").table("bar").get(1).replace( function(doc) {
		    return r.branch(
		        doc.eq(null),
		        newDoc,
		        doc.merge({ doc("results").add(newDoc("results")) })
		    )
		})
	*/

	newTopic := Topic{
		ID:      topic,
		Clients: []string{clientid},
	}

	response, err := r.Table(rt.TopicTable).Get(topic).Replace(func(doc r.Term) interface{} {
		return r.Branch(doc.Eq(nil),
			newTopic,
			doc.Merge(map[string]interface{}{"clients": doc.Field("clients").Add(newTopic.Clients)}))
	}).RunWrite(rt.Session)
	if err != nil {
		return fmt.Errorf("Error appending data: %s", err.Error())
	}

	if response.Errors >= 1 {
		return fmt.Errorf("Error appending data: %s", response.FirstError)
	}

	return nil
}

//Unsubscribe
func (rt *RethinkStorage) Unsubscribe(topic string, clientid string) error {

	response, err := r.Table(rt.TopicTable).
		Get(topic).
		Update(map[string]interface{}{"clients": r.Row.Field("clients").SetDifference([]string{clientid})}).
		RunWrite(rt.Session)

	if err != nil {
		return fmt.Errorf("Error deleting data: %s", err.Error())
	}
	if response.Errors >= 1 {
		return fmt.Errorf("Error deleting data: %s", response.FirstError)
	}

	return nil

}

//UpdateRetain
func (rt *RethinkStorage) UpdateRetain(topic string, m *proto.Publish) error {

	rdata := Retain{
		Topic: topic,
		Msg:   m,
	}

	response, err := r.Table(rt.RetainTable).Insert(rdata, r.InsertOpts{Conflict: "update"}).RunWrite(rt.Session)
	if err != nil {
		return fmt.Errorf("Error inserting data: %s", err.Error())
	}

	if response.Errors >= 1 {
		return fmt.Errorf("Error inserting data: %s", response.FirstError)
	}

	return nil

}

//GetRetain
func (rt *RethinkStorage) GetRetain(topic string) (*proto.Publish, error) {

	cursor, err := r.Table(rt.RetainTable).
		Get(topic).
		Run(rt.Session)
	if err != nil {
		return nil, fmt.Errorf("Error Finding by ID: %s", err)
	}
	defer cursor.Close()

	if cursor.IsNil() {
		return nil, fmt.Errorf("Error Entry not found")
	}
	var result Retain
	//var result interface{}
	//var result map[string]interface{}
	err = cursor.One(&result)
	if err == r.ErrEmptyResult {
		return nil, fmt.Errorf("Error:%s", r.ErrEmptyResult.Error()) // row not found
	}
	if err != nil {
		return nil, fmt.Errorf("Error scanning db result: %s", err)
	}

	log.Printf("%v", result.Msg)
	return result.Msg, nil
}

func (r *Retain) UnmarshalRQL(data interface{}) error {
	log.Printf("data %v %T", data, data)

	//var rData map[string]interface{}

	rData, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("pseudo-type Retain object is not valid")
	}

	log.Printf("[DEBUG] %T - %v\n", rData, rData)

	//if rData, ok := rData.(*retain); ok {
	topic, ok := rData["topic"].(string)
	if !ok {
		return fmt.Errorf("Topic is not valid string")
	}
	r.Topic = topic
	newMsg := &proto.Publish{}
	r.Msg = newMsg

	msg, ok := rData["msg"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("Msg is not valid map[string]interface{} type")
	}
	//r.Msg = &msg
	log.Printf("msg %T - %v\n", msg, msg)
	log.Printf("msg %T - %v\n", msg["TopicName"], msg["TopicName"])

	r.Msg.TopicName = msg["TopicName"].(string)
	qos := msg["QosLevel"].(float64)

	//switch?
	if qos == 0 {
		r.Msg.QosLevel = proto.QosAtMostOnce
	} else if qos == 1 {
		r.Msg.QosLevel = proto.QosAtLeastOnce
	} else if qos == 2 {
		r.Msg.QosLevel = proto.QosExactlyOnce
	} else {
		return fmt.Errorf("QosLevel is not valid QosLevel type")
	}

	r.Msg.DupFlag = msg["DupFlag"].(bool)
	r.Msg.Retain = msg["Retain"].(bool)
	msgID := msg["MessageId"].(float64)
	r.Msg.MessageId = uint16(msgID)

	log.Printf("payload %v", msg["Payload"])

	payloadArray := msg["Payload"].([]uint8)
	w := bytes.NewBuffer(payloadArray)

	pay := &proto.BytesPayload{}
	r.Msg.Payload = pay
	r.Msg.Payload.WritePayload(w)

	return nil

}

func NewRethinkStorage(url, port, dbname, authkey string) *RethinkStorage {
	s := &RethinkStorage{
		ClientTable:    "mqttClients",
		TopicTable:     "mqttTopics",
		RetainTable:    "mqttRetain",
		StoredMsgTable: "mqttStoredMsg",
		Url:            url,
		Port:           port,
		DbName:         dbname,
		AuthKey:        authkey,
	}
	s.Conn()
	return s
}
