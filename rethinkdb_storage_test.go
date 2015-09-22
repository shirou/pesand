package main

import (
	proto "github.com/huin/mqtt"
	"reflect"
	"testing"
)

func TestRethinkUpdateRetain(t *testing.T) {
	//var topic string
	topic := "/a/b/c"
	// Load config file
	conf, err := NewConfig("pesand.conf")
	if err != nil {
		t.Errorf("could not load config: %s", err)
	}
	db := NewRethinkStorage(conf.RethinkDB.Url, conf.RethinkDB.Port, conf.RethinkDB.DbName, conf.RethinkDB.AuthKey)

	origm := &proto.Publish{
		Header: proto.Header{
			DupFlag:  false,
			QosLevel: proto.QosAtMostOnce,
			Retain:   false,
		},
		TopicName: "/a/b/c",
		Payload:   proto.BytesPayload{1, 2, 3},
	}
	err = db.UpdateRetain(topic, origm)
	if err != nil {
		t.Errorf("could not update: %s", err)
	}
	if m, err := db.GetRetain(topic); err == nil {
		if origm.TopicName != m.TopicName {
			t.Errorf("could not get update: %v", topic)
		}
	} else {
		t.Errorf("could not get update %v topic: %v", err, topic)
	}

	// Same topic Again
	origm.MessageId = 0x1234
	db.UpdateRetain(topic, origm)
	if m, err := db.GetRetain(topic); err == nil {
		if m.MessageId != 0x1234 {
			t.Errorf("could not update: %v", topic)
		}
	} else {
		t.Errorf("could not update %v topic: %v", err, topic)
	}

	// Other topic
	topic2 := "/other/topic"
	m2 := &proto.Publish{
		Header: proto.Header{
			DupFlag:  false,
			QosLevel: proto.QosAtMostOnce,
			Retain:   false,
		},
		TopicName: topic2,
		Payload:   proto.BytesPayload{1, 2, 3},
	}
	db.UpdateRetain(topic2, m2)
	if m, err := db.GetRetain(topic2); err == nil {
		if m.TopicName != topic2 {
			t.Errorf("could not update: %v", topic2)
		}
		// check existing topic
		if mm, err := db.GetRetain(topic); err == nil {
			if mm.TopicName == m.TopicName {
				t.Errorf("Duplicated message")
			}
		} else {
			t.Errorf("could not update after other topic: %v", topic)
		}

	} else {
		t.Errorf("could not update: %v", topic2)
	}

}

func TestRethinkSubscribe(t *testing.T) {
	var topic string
	var cid string
	var cid2 string
	conf, err := NewConfig("pesand.conf")
	if err != nil {
		t.Errorf("could not load config: %s", err)
	}
	db := NewRethinkStorage(conf.RethinkDB.Url, conf.RethinkDB.Port, conf.RethinkDB.DbName, conf.RethinkDB.AuthKey)

	topic = "/a/b/c"

	cid = "cid1"
	db.Subscribe(topic, cid)
	val, _ := db.GetTopicClientList(topic)
	if reflect.DeepEqual(val, []string{cid}) != true {
		t.Errorf("clientid not found: %v", cid)
	}

	cid2 = "cid2"
	db.Subscribe(topic, cid2)
	val, _ = db.GetTopicClientList(topic)
	if reflect.DeepEqual(val, []string{cid, cid2}) != true {
		t.Errorf("clientid not found: %v", cid2)
	}

	db.Unsubscribe(topic, cid)
	val, _ = db.GetTopicClientList(topic)
	for _, c := range val {
		if c == cid {
			t.Errorf("Unsubscribed id found: %v", cid)
		}
	}

	db.Unsubscribe(topic, cid2)
	val, _ = db.GetTopicClientList(topic)
	for _, c := range val {
		if c == cid2 {
			t.Errorf("Unsubscribed id found: %v", cid2)
		}
	}

}

/*
func TestBoltCreateStoredMsgId(t *testing.T) {
	m := &proto.Publish{
		Header: proto.Header{
			DupFlag:  false,
			QosLevel: proto.QosAtMostOnce,
			Retain:   false,
		},
		TopicName: "/a/b/c",
		Payload:   proto.BytesPayload{1, 2, 3},
	}
	if createStoredMsgId("a", m) != "a-0" {
		t.Errorf("StoredMsgId creating failed with nomsg")
	}

	var msgid uint16
	msgid = 1000
	m2 := &proto.Publish{
		Header: proto.Header{
			DupFlag:  false,
			QosLevel: proto.QosAtMostOnce,
			Retain:   false,
		},
		MessageId: msgid,
		TopicName: "/a/b/c",
		Payload:   proto.BytesPayload{1, 2, 3},
	}
	if createStoredMsgId("a", m2) != "a-1000" {
		t.Errorf("StoredMsgId creating failed")
	}

}*/
