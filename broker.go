package main

import (
	//"github.com/golang/glog"
	log "github.com/Sirupsen/logrus"
	proto "github.com/huin/mqtt"
	"net"
	"time"
)

type Broker struct {
	listen           net.Listener
	Port             string
	stats            Stats
	storage          Storage
	auth             Authorize
	conf             *Config
	Done             chan struct{}
	StatsIntervalSec time.Duration
}

func NewBroker(conf Config, listen net.Listener) *Broker {
	broker := &Broker{
		listen:           listen,
		Port:             conf.Default.Port,
		stats:            NewStats(),
		storage:          NewMemStorage(),
		auth:             NewJWTAuth(conf.Auth.JWTKey),
		conf:             &conf,
		Done:             make(chan struct{}),
		StatsIntervalSec: time.Second * conf.Default.StatsIntervalSec,
	}
	return broker
}

// Auth auth
func (b *Broker) Auth(username string, password string) bool {
	return b.auth.Auth(username, password)
}

func (b *Broker) Start() {

	log.Info("Broker started")
	go func() {
		for {
			conn, err := b.listen.Accept()
			if err != nil {
				log.Warnf("Accept: %v", err)
				break
			}

			c := NewConnection(b, conn)
			c.Start()
			b.stats.clientConnect()
		}
		close(b.Done)
	}()

	// start the stats reporting goroutine
	go func() {
		for {
			for _, m := range b.stats.GetStatsMessages(b.StatsIntervalSec) {
				b.Publish(m)
			}
			select {
			case <-b.Done:
				return
			default:
				// keep going
			}
			time.Sleep(b.StatsIntervalSec)
		}
	}()

}

func (b *Broker) Publish(m *proto.Publish) {
	topic := m.TopicName
	topics, _ := ExpandTopics(topic)
	for _, t := range topics {
		go func(t string) {
			for _, clientid := range b.storage.GetTopicClientList(t) {
				conn := b.storage.GetClientConnection(clientid)
				conn.submit(m)
				b.stats.messageSend()
			}
		}(t)
	}
}

func (b *Broker) UpdateRetain(m *proto.Publish) {
	topics, _ := ExpandTopics(m.TopicName)
	for _, t := range topics {
		b.storage.UpdateRetain(t, m)
	}
}
func (b *Broker) GetRetain(topic string) (*proto.Publish, bool) {
	m, ok := b.storage.GetRetain(topic)
	return m, ok
}

func (b *Broker) Subscribe(topic string, conn *Connection) {

	log.WithFields(log.Fields{"topic": topic, "clientID": conn.clientid}).
		Info("Subscribe")

	b.storage.Subscribe(topic, conn.clientid)
}

func (b *Broker) Unsubscribe(topic string, conn *Connection) {

	log.WithFields(log.Fields{"topic": topic, "clientID": conn.clientid}).
		Info("Unsubscribe")

	topics, _ := ExpandTopics(topic)
	for _, t := range topics {
		b.storage.Unsubscribe(t, conn.clientid)
	}
}
