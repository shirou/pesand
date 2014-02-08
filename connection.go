package main

import (
	//	"bufio"
	"errors"
	//	proto "github.com/shirou/mqtt"
	proto "github.com/huin/mqtt"
	"io"
	"log"
	"net"
	"strings"
)

// ConnectionErrors is an array of errors corresponding to the
// Connect return codes specified in the specification.
var ConnectionErrors = [6]error{
	nil, // Connection Accepted (not an error)
	errors.New("Connection Refused: unacceptable protocol version"),
	errors.New("Connection Refused: identifier rejected"),
	errors.New("Connection Refused: server unavailable"),
	errors.New("Connection Refused: bad user name or password"),
	errors.New("Connection Refused: not authorized"),
}

const (
	ClientAvailable   int = 0
	ClientUnAvailable     // no PINGACK, no DISCONNECT
)

type Connection struct {
	broker   *Broker
	conn     net.Conn
	clientid string
	storage  Storage
	jobs     chan job
	Done     chan struct{}
	Status   int
}

type job struct {
	m proto.Message
	r receipt
}

type receipt chan struct{}

// Wait for the receipt to indicate that the job is done.
func (r receipt) wait() {
	// TODO: timeout
	<-r
}

func (c *Connection) handleConnection() {
	defer func() {
		c.conn.Close()
		c.broker.stats.clientDisconnect(c)
		close(c.jobs)
	}()

	for {
		m, err := proto.DecodeOneMessage(c.conn, nil)
		if err != nil {
			if err == io.EOF {
				return
			}
			if strings.HasSuffix(err.Error(), "use of closed network connection") {
				return
			}
			log.Print("reader: ", err)
			return
		}
		log.Printf("incoming: %T", m)
		switch m := m.(type) {
		case *proto.Connect:
			c.handleConnect(m)
		case *proto.Publish:
			c.handlePublish(m)
		case *proto.PingReq:
			c.submit(&proto.PingResp{})
		case *proto.Disconnect:
			// finish this goroutine
			return
		case *proto.Subscribe:
			c.handleSubscribe(m)
		case *proto.Unsubscribe:
			c.handleUnsubscribe(m)
		default:
			log.Printf("reader: unknown msg type %T, continue anyway", m)
			continue
		}
	}
}

func (c *Connection) handleSubscribe(m *proto.Subscribe) {
	if m.Header.QosLevel != proto.QosAtLeastOnce {
		// protocol error, disconnect
		return
	}
	suback := &proto.SubAck{
		MessageId: m.MessageId,
		TopicsQos: make([]proto.QosLevel, len(m.Topics)),
	}
	for i, tq := range m.Topics {
		// TODO: Handle varying QoS correctly
		c.broker.Subscribe(tq.Topic, c)
		suback.TopicsQos[i] = proto.QosAtMostOnce
	}
	c.submit(suback)
	// Process retained messages.
	for _, tq := range m.Topics {
		if pubmsg, ok := c.broker.storage.GetRetain(tq.Topic); ok {
			c.submit(pubmsg)
		}
	}
}

func (c *Connection) handleUnsubscribe(m *proto.Unsubscribe) {
	for _, t := range m.Topics {
		c.broker.Unsubscribe(t, c)
	}
	ack := &proto.UnsubAck{MessageId: m.MessageId}
	c.submit(ack)
}

func (c *Connection) handleConnect(m *proto.Connect) {
	rc := proto.RetCodeAccepted
	if m.ProtocolName != "MQIsdp" ||
		m.ProtocolVersion != 3 {
		log.Print("reader: reject connection from ", m.ProtocolName, " version ", m.ProtocolVersion)
		rc = proto.RetCodeUnacceptableProtocolVersion
	}

	// Check client id.
	if len(m.ClientId) < 1 || len(m.ClientId) > 23 {
		rc = proto.RetCodeIdentifierRejected
	}
	c.clientid = m.ClientId

	currrent_c, err := c.storage.MergeClient(c.clientid, c)
	if err != nil {
		c.storage.DeleteClient(c.clientid, c)
		return
	}

	// TODO: Last will
	connack := &proto.ConnAck{
		ReturnCode: rc,
	}

	currrent_c.submit(connack)

	// close connection if it was a bad connect
	if rc != proto.RetCodeAccepted {
		log.Printf("Connection refused for %v: %v", currrent_c.conn.RemoteAddr(), ConnectionErrors[rc])
		return
	}

	// Log in mosquitto format.
	clean := 0
	if m.CleanSession {
		clean = 1
	}
	log.Printf("New client connected from %v as %v (c%v, k%v).", currrent_c.conn.RemoteAddr(), currrent_c.clientid, clean, m.KeepAliveTimer)
}

func (c *Connection) handlePublish(m *proto.Publish) {
	// TODO: Proper QoS support
	if m.Header.QosLevel != proto.QosAtMostOnce {
		log.Printf("reader: no support for QoS %v yet", m.Header.QosLevel)
		return
	}
	c.broker.Publish(m)

	if m.Header.Retain {
		c.broker.UpdateRetain(m)
		log.Printf("Publish msg retained: %s", m.TopicName)
	}
	c.submit(&proto.PubAck{MessageId: m.MessageId})
}

// Queue a message; no notification of sending is done.
func (c *Connection) submit(m proto.Message) {
	j := job{m: m}
	select {
	case c.jobs <- j:
	default:
		log.Print(c, ": failed to submit message")
	}
	return
}

// Queue a message, returns a channel that will be readable
// when the message is sent.
func (c *Connection) submitSync(m proto.Message) receipt {
	j := job{m: m, r: make(receipt)}
	c.jobs <- j
	return j.r
}

func (c *Connection) writer() {

	// Close connection on exit in order to cause reader to exit.
	defer func() {
		c.conn.Close()
		c.storage.DeleteClient(c.clientid, c)
		//		c.svr.subs.unsubAll(c)
	}()

	for job := range c.jobs {
		// TODO: write timeout
		err := job.m.Encode(c.conn)
		if job.r != nil {
			// notifiy the sender that this message is sent
			close(job.r)
		}
		if err != nil {
			// This one is not interesting; it happens when clients
			// disappear before we send their acks.
			if err.Error() == "use of closed network connection" {
				return
			}
			log.Print("writer: ", err)
			return
		}
		//		c.svr.stats.messageSend()

		if _, ok := job.m.(*proto.Disconnect); ok {
			log.Print("writer: sent disconnect message")
			return
		}
	}
}

func (c *Connection) Start() {
	go c.handleConnection()
	go c.writer()

	/*
		defer c.conn.Close()
		reader := bufio.NewReader(c.conn)
		for {
			_, err := reader.ReadByte()
			if err != nil {
				return
			}
		}
	*/
}

func NewConnection(b *Broker, conn net.Conn) *Connection {
	c := &Connection{
		broker:  b,
		conn:    conn,
		storage: b.storage,
		jobs:    make(chan job, b.conf.Queue.SendingQueueLength),
		Status:  ClientAvailable,
		//		out:      make(chan job, clientQueueLength),
		//		Incoming: make(chan *proto.Publish, clientQueueLength),
		//		done:     make(chan struct{}),
		//		connack:  make(chan *proto.ConnAck),
		//		suback:   make(chan *proto.SubAck),
	}
	return c
}
