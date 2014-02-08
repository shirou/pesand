package main

import (
	proto "github.com/huin/mqtt"
	"log"
	"net"
	"time"
)

type Broker struct {
	listen        net.Listener
	Port          string
	stats         Stats
	storage       Storage
	conf          *Config
	Done          chan struct{}
	StatsInterval time.Duration
}

func NewBroker(conf Config, listen net.Listener) *Broker {
	broker := &Broker{
		listen:        listen,
		Port:          conf.Default.Port,
		stats:         NewStats(),
		storage:       NewMemStorage(),
		conf:          &conf,
		Done:          make(chan struct{}),
		StatsInterval: time.Second * 10,
	}
	return broker
}

func (b *Broker) Start() {
	go func() {
		for {
			conn, err := b.listen.Accept()
			if err != nil {
				log.Print("Accept: ", err)
				break
			}
			c := NewConnection(b, conn)
			c.Start()
		}
		close(b.Done)
	}()
}

func (b *Broker) Publish(m *proto.Publish) {
	topic := m.TopicName
	topics, _ := ExpandTopics(topic)
	log.Println(topics)
	for _, t := range topics {
		go func(t string) {
			for _, clientid := range b.storage.GetTopicClientList(t) {
				conn := b.storage.GetClientConnection(clientid)
				conn.submit(m)
			}
		}(t)
	}
}

func (b *Broker) Subscribe(topic string, conn *Connection) {
	log.Printf("Subscribe: %s on %s", topic, conn.clientid)
	b.storage.Subscribe(topic, conn.clientid)
}
