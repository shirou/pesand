package main

import (
	"strings"
)

// When Publish, add clientid to topic as a key with wild card
// ex:
//   key: "/a/b/c"  value: [clientid1, clientid2 ...]
//   key: "/a/b/#"  value: [clientid1, clientid2 ...]
// When publish, expand topic with possible wild card
// ex:
//   "/a/b/c" ->
//  ["/a/b/c", "/#", "/a/#", "/a/b/#", "/+/b/c", "/a/+/c", "/a/b/+" ... ]
//
// when 5 level, 1 + 5 + 5 = 11 topic key.
//
// The MQTT specification allows multiple plus such as "/a/+/+/c",
// If follow this, topic key length will be 1 + 5 + 5^2.
// However, this implementation asumes such a topic is not so many,
// we stores such topic aside. (but not implemented yet)
//
// DANGER: this is a very naive implementation
//  1. wild card values on a key may becomes too large.
//  2. if topic level is too deep, should be slow
// but it should be fast and kvs friendly, I think

// Cache
var TopicExpandCache = make(map[string][]string, 0)

// topic key which includes multiple plus, and get reference counts
// to delete later.
// TODO: should be persistent?
// TODO: decrease count and delete function.
var MultiPlusWildCard = make(map[string][]string, 0)

func IsValidTopic(topic string) bool {
	// A topic must be at least one character long.
	if len(topic) == 0 || len(topic) > 65535 {
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

// TODO: Not Implemented yet.
func IsMultiPlusTopic(topic string) ([]string, bool) {
	ret := make([]string, 0)
	t := strings.Split(topic, "/")
	// first part is used as a key
	for _, sub := range MultiPlusWildCard[t[0]] {
		bingo := 0
		for i, part := range strings.Split(sub, "/") {
			if part == "#" || part == t[i] {
				bingo += 1
			}
		}

		if bingo == len(t) {
			ret = append(ret, sub)
		}
	}
	if len(ret) > 0 {
		return ret, true
	} else {
		return ret, false
	}
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

	ret := []string{topic, "#"}
	sp := []string{}
	topic = strings.TrimSpace(topic)

	if t, ok := IsMultiPlusTopic(topic); ok {
		ret = append(ret, t...)
	}

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
