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
//  ["/a/b/c", "/#", "/a/#", "/a/b/#", "/+/b/c", "/a/+/c", "/a/b/+" ... ]

// when 5 level, 1 + 5 + 2^5 = 38 topic key.

// DANGER: this is a very naive implementation
//  1. wild card values on a key may becomes too large.
//  2. if topic level is too deep, should be slow
// but it should be fast and kvs friendly, I think

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
	var has_leading_slash bool
	if strings.HasPrefix(topic, "/") {
		has_leading_slash = true
	} else {
		topic = "/" + topic
		has_leading_slash = false
	}

	topic = strings.TrimSpace(topic)
	ret := []string{topic, "#"}
	sp := []string{}
	// cleate topic list and delete empty.
	for _, t := range strings.Split(topic, "/") {
		if len(t) > 0 {
			sp = append(sp, t)
		}
	}
	// add num wildcard to ret
	tmp := make([]string, 0)
	for i := 0; i < len(sp); i++ {
		tmp = append(tmp, sp[i], "#")
		ret = append(ret, strings.Join(tmp, "/"))
		tmp = tmp[0 : len(tmp)-1] // remove last "#"
	}

	// add plus wildcard
	// TODO: multiple plus wildcard
	for i := 0; i < (len(sp) - 1); i++ {
		tmp := make([]string, len(sp))
		copy(tmp, sp)
		tmp[i] = "+"
		ret = append(ret, strings.Join(tmp, "/"))
	}

	for i, s := range ret {
		if has_leading_slash == true {
			if strings.HasPrefix(s, "/") == false {
				ret[i] = "/" + s
			}
		} else {
			// delete first "/"
			ret[i] = strings.TrimLeft(s, "/")
		}
	}

	TopicExpandCache[topic] = ret

	return ret, nil
}
