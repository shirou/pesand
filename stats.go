package main

import (
	proto "github.com/huin/mqtt"
	"sync/atomic"
	"time"
)

type Stats struct {
	recv       int64
	sent       int64
	clients    int64
	clientsMax int64
	lastmsgs   int64
}

func (s *Stats) messageRecv()      { atomic.AddInt64(&s.recv, 1) }
func (s *Stats) messageSend()      { atomic.AddInt64(&s.sent, 1) }
func (s *Stats) clientConnect()    { atomic.AddInt64(&s.clients, 1) }
func (s *Stats) clientDisconnect() { atomic.AddInt64(&s.clients, -1) }

func statsMessage(topic string, stat int64) *proto.Publish {
	header := proto.Header{
		DupFlag:  false,
		QosLevel: proto.QosAtMostOnce,
		Retain:   true,
	}

	return &proto.Publish{
		Header:    header,
		TopicName: topic,
		Payload:   newIntPayload(stat),
	}
}

func (s *Stats) GetStatsMessages(interval time.Duration) []*proto.Publish {
	ret := make([]*proto.Publish, 0)
	clients := atomic.LoadInt64(&s.clients)
	clientsMax := atomic.LoadInt64(&s.clientsMax)
	if clients > clientsMax {
		clientsMax = clients
		atomic.StoreInt64(&s.clientsMax, clientsMax)
	}

	ret = append(ret, statsMessage("$SYS/broker/clients/active", clients))
	ret = append(ret, statsMessage("$SYS/broker/clients/maximum", clientsMax))
	ret = append(ret, statsMessage("$SYS/broker/messages/received",
		atomic.LoadInt64(&s.recv)))
	ret = append(ret, statsMessage("$SYS/broker/messages/sent",
		atomic.LoadInt64(&s.sent)))

	msgs := atomic.LoadInt64(&s.recv) + atomic.LoadInt64(&s.recv)
	msgpersec := (msgs - s.lastmsgs) / int64(interval/time.Second)
	// no need for atomic because we are the only reader/writer of it
	s.lastmsgs = msgs

	ret = append(ret, statsMessage("$SYS/broker/messages/per-sec", msgpersec))

	return ret
}

func NewStats() Stats {
	return Stats{}
}
