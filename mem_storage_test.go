package main

import (
	proto "github.com/huin/mqtt"
	"testing"
)

func TestUpdateRetain(t *testing.T) {
	var topic string
	mem := NewMemStorage()

	origm := &proto.Publish{
		Header: proto.Header{
			DupFlag:  false,
			QosLevel: proto.QosAtMostOnce,
			Retain:   false,
		},
		TopicName: "/a/b/c",
		Payload:   proto.BytesPayload{1, 2, 3},
	}
	mem.UpdateRetain(topic, origm)
	if m, ok := mem.GetRetain(topic); ok {
		if origm.TopicName != m.TopicName {
			t.Errorf("could not update: %v", topic)
		}
	} else {
		t.Errorf("could not update: %v", topic)
	}

	// Same topic Again
	origm.MessageId = 0x1234
	mem.UpdateRetain(topic, origm)
	if m, ok := mem.GetRetain(topic); ok {
		if m.MessageId != 0x1234 {
			t.Errorf("could not update: %v", topic)
		}
	} else {
		t.Errorf("could not update: %v", topic)
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
	mem.UpdateRetain(topic2, m2)
	if m, ok := mem.GetRetain(topic2); ok {
		if m.TopicName != topic2 {
			t.Errorf("could not update: %v", topic2)
		}
		// check existing topic
		if mm, ok := mem.GetRetain(topic); ok {
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

func TestGetRetain(t *testing.T) {
	//	var topic string

	//	topic = "/a/b/c"
}
