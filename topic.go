package main

import (
	"strings"
)

// When subscribe, add clientid to topic as a key with wild card
// ex:
//   key: "/a/b/c"  value: [clientid1, clientid2 ...]
//   key: "/a/b/#"  value: [clientid1, clientid2 ...]
// When publish, expand topic with possible wild card
// ex:
//   "/a/b/c" ->
//  ["/a/b/c", "/#", "/a/#", "/a/b/#", "/+/b/c", "/a/+/c", "/a/b/+"]

// when 5 level, 1 + 5 + 5 = 11 topic key.

// DANGER: this is a very naive implementation
//  1. wild card values may becomes too large
//  2. if too deep, should be slow
// but it should fast and kvs friendly, I think

// Cache
var TopicExpandCache = make(map[string][]string, 0)

func IsValidTopic(topic string) bool {
	// A topic must be at least one character long.
	if len(topic) == 0 {
		return false
	}

	// check multiple wildcard
	if strings.Count(topic, "#") > 1 {
		return false
	}
	// exists but not last
	num_i := strings.Index(topic, "#")
	if num_i > -1 && num_i != len(topic)-1 {
		return false
	}

	// TODO: check more strict

	return true
}

func ExpandTopics(topic string) ([]string, error) {
	if _, ok := TopicExpandCache[topic]; ok {
		return TopicExpandCache[topic], nil
	}

	ret := []string{topic}
	sp := []string{}
	// clete topic list and delete empty.
	for _, t := range strings.Split(topic, "/") {
		if len(t) > 0 {
			sp = append(sp, t)
		}
	}
	// add num wildcard
	tmp := make([]string, 0)
	for i := 0; i < len(sp); i++ {
		tmp = append(tmp, sp[i], "#")
		ret = append(ret, "/"+strings.Join(tmp, "/"))
		tmp = tmp[0 : len(tmp)-1] // remove last "#"
	}

	// add plus wildcard TODO: multiple plus wildcard
	for i := 0; i < (len(sp) - 1); i++ {
		tmp := make([]string, len(sp))
		copy(tmp, sp)
		tmp[i] = "+"
		ret = append(ret, "/"+strings.Join(tmp, "/"))
	}

	TopicExpandCache[topic] = ret

	return ret, nil
}
