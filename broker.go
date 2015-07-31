package main

import (
	log "github.com/Sirupsen/logrus"
	proto "github.com/huin/mqtt"
	"net"
	"sync"
	"time"
)

type Broker struct {
	listen           net.Listener
	Port             string
	stats            Stats
	storage          Storage
	auth             Authorize
	clientsMu        sync.Mutex
	clients          map[string]*Connection
	conf             *Config
	Done             chan struct{}
	StatsIntervalSec time.Duration
}

func NewBroker(conf Config, listen net.Listener) *Broker {
	broker := &Broker{
		listen:           listen,
		Port:             conf.Default.Port,
		stats:            NewStats(),
		storage:          conf.ConfigStorage(),
		auth:             conf.ConfigAuth(),
		clients:          make(map[string]*Connection),
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
func (b *Broker) AllowAnon() bool {
	return b.auth.AllowAnon()
}

//Start
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

//Publish
func (b *Broker) Publish(m *proto.Publish) {
	topic := m.TopicName
	topics, _ := ExpandTopics(topic)
	for _, t := range topics {

		go func(t string) {

			list, err := b.storage.GetTopicClientList(t)
			if err != nil {
				log.WithField("topic", t).
					Error(err)
			}

			for _, clientid := range list {
				//sess, err := b.storage.GetClientSession(clientid)
				b.clientsMu.Lock()
				conn, ok := b.clients[clientid]
				for k, _ := range b.clients {
					log.Println(k)
				}
				b.clientsMu.Unlock()

				if !ok {
					log.WithFields(log.Fields{"topic": t, "clientID": clientid}).
						Error("clientsList conn not ok")
					continue
				}
				conn.submit(m)
				b.stats.messageSend()
			}
		}(t)
	}
}

//UpdateRetain
func (b *Broker) UpdateRetain(m *proto.Publish) {
	topics, _ := ExpandTopics(m.TopicName)
	for _, t := range topics {
		err := b.storage.UpdateRetain(t, m)
		if err != nil {
			log.WithField("topic", t).
				Errorf("UpdateRetain error %v", err)
		}
	}
}

//GetRetain
func (b *Broker) GetRetain(topic string) (*proto.Publish, error) {
	m, err := b.storage.GetRetain(topic)
	return m, err
}

//Subscribe
func (b *Broker) Subscribe(topic string, sess *Session) {

	log.WithFields(log.Fields{"topic": topic, "clientID": sess.Clientid}).
		Info("Subscribe")

	err := b.storage.Subscribe(topic, sess.Clientid)
	if err != nil {
		log.WithFields(log.Fields{"topic": topic, "clientID": sess.Clientid}).
			Errorf("Subscribe error %v", err)
	}
}

//Unsubscribe
func (b *Broker) Unsubscribe(topic string, sess *Session) {

	log.WithFields(log.Fields{"topic": topic, "clientID": sess.Clientid}).
		Info("Unsubscribe")

	topics, _ := ExpandTopics(topic)
	for _, t := range topics {
		err := b.storage.Unsubscribe(t, sess.Clientid)
		if err != nil {
			log.WithFields(log.Fields{"topic": topic, "clientID": sess.Clientid}).
				Errorf("Unsubscribe error %v", err)
		}

	}
}
