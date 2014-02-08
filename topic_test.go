package main

import (
	"fmt"
	"testing"
)

func TestIsValidTopic(t *testing.T) {
	var topic string

	// OK
	topic = "/a/b/c"
	if IsValidTopic(topic) == false {
		t.Errorf("got false: %v", topic)
	}
	topic = "/a/b/#"
	if IsValidTopic(topic) == false {
		t.Errorf("got false: %v", topic)
	}
	topic = "/a/b/+"
	if IsValidTopic(topic) == false {
		t.Errorf("got false: %v", topic)
	}
	topic = "/a/+/c"
	if IsValidTopic(topic) == false {
		t.Errorf("got false: %v", topic)
	}
	topic = "/a/+/+"
	if IsValidTopic(topic) == false {
		t.Errorf("got false: %v", topic)
	}

	// NG
	topic = "/a/#/#"
	if IsValidTopic(topic) == true {
		t.Errorf("got true: %v", topic)
	}
	topic = "/a/#/c"
	if IsValidTopic(topic) == true {
		t.Errorf("got true: %v", topic)
	}

}

func TestExpandTopic(t *testing.T) {
	var topic string

	topic = "/a/b/c"
	ret, _ := ExpandTopics(topic)
	fmt.Println(ret)
}
