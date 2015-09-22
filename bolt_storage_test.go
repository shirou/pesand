package main

import (
	proto "github.com/huin/mqtt"
	"reflect"
	"testing"
)

func TestBoltUpdateRetain(t *testing.T) {
	//var topic string
	topic := "/a/b/c"
	bolt := NewBoltStorage("pesand.db")
	defer bolt.Close()

	origm := &proto.Publish{
		Header: proto.Header{
			DupFlag:  false,
			QosLevel: proto.QosAtMostOnce,
			Retain:   false,
		},
		TopicName: "/a/b/c",
		Payload:   proto.BytesPayload{1, 2, 3},
	}
	err := bolt.UpdateRetain(topic, origm)
	if err != nil {
		t.Errorf("could not update: %s", err)
	}
	if m, err := bolt.GetRetain(topic); err == nil {
		if origm.TopicName != m.TopicName {
			t.Errorf("could not get update: %v", topic)
		}
	} else {
		t.Errorf("could not get update %v topic: %v", err, topic)
	}

	// Same topic Again
	origm.MessageId = 0x1234
	bolt.UpdateRetain(topic, origm)
	if m, err := bolt.GetRetain(topic); err == nil {
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
	bolt.UpdateRetain(topic2, m2)
	if m, err := bolt.GetRetain(topic2); err == nil {
		if m.TopicName != topic2 {
			t.Errorf("could not update: %v", topic2)
		}
		// check existing topic
		if mm, err := bolt.GetRetain(topic); err == nil {
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

func TestBoltSubscribe(t *testing.T) {
	var topic string
	var cid string
	var cid2 string
	bolt := NewBoltStorage("pesand.db")
	defer bolt.Close()

	topic = "/a/b/c"

	cid = "cid1"
	bolt.Subscribe(topic, cid)
	val, _ := bolt.GetTopicClientList(topic)
	if reflect.DeepEqual(val, []string{cid}) != true {
		t.Errorf("clientid not found: %v", cid)
	}

	cid2 = "cid2"
	bolt.Subscribe(topic, cid2)
	val, _ = bolt.GetTopicClientList(topic)
	if reflect.DeepEqual(val, []string{cid, cid2}) != true {
		t.Errorf("clientid not found: %v", cid2)
	}

	bolt.Unsubscribe(topic, cid)
	val, _ = bolt.GetTopicClientList(topic)
	for _, c := range val {
		if c == cid {
			t.Errorf("Unsubscribed id found: %v", cid)
		}
	}

	bolt.Unsubscribe(topic, cid2)
	val, _ = bolt.GetTopicClientList(topic)
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
