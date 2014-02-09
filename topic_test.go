package main

import (
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

func checkInclude(target []string, s string) bool {
	for _, c := range target {
		if c == s {
			return true
		}
	}
	return false
}

func TestExpandTopic(t *testing.T) {
	var topic string

	topic = "/a/b/c"
	ret, _ := ExpandTopics(topic)
	expected := []string{"/a/b/c", "/#", "/a/#", "/a/b/#", "/a/+/c"}
	for _, s := range expected {
		if checkInclude(ret, s) != true {
			t.Errorf("%v is not included in %v", s, ret)
		}
	}

	// not leading "/"
	topic = "a/b/c"
	ret, _ = ExpandTopics(topic)
	expected = []string{"a/b/c", "#", "a/#", "a/b/#", "a/+/c"}
	for _, s := range expected {
		if checkInclude(ret, s) != true {
			t.Errorf("%v is not included in %v", s, ret)
		}
	}

}
