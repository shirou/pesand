package main

import (
	"errors"
	//	proto "github.com/shirou/mqtt"
	proto "github.com/huin/mqtt"
	"log"
	"net"
	"sync"
	"time"
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
	ClientAvailable   uint8 = iota
	ClientUnAvailable       // no PINGACK, no DISCONNECT
	ClientDisconnectedNormally
)

type Connection struct {
	broker            *Broker
	conn              net.Conn
	clientid          string
	storage           Storage
	jobs              chan job
	Done              chan struct{}
	Status            uint8
	TopicList         []string // Subscribed topic list
	LastUpdated       time.Time
	SendingMsgs       *StoredQueue // msgs which not sent
	SentMsgs          *StoredQueue // msgs which already sent
	WillMsg           *proto.Publish
	KeepAliveTimer    uint16
	lastKeepAliveTime time.Time
	Username          string
}

type job struct {
	m           proto.Message
	r           receipt
	storedmsgid string
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
		close(c.jobs)
	}()

	for {
		m, err := proto.DecodeOneMessage(c.conn, nil)
		if err != nil {
			log.Printf("disconnected unexpectedly (%s): %s", c.clientid, err)

			if c.WillMsg != nil {
				log.Printf("Send Will message of %s", c.clientid)
				c.handlePublish(c.WillMsg)
			}

			c.Status = ClientUnAvailable
			return
		}
		log.Printf("incoming: %T from %v", m, c.clientid)
		switch m := m.(type) {
		case *proto.Connect:
			c.handleConnect(m)
		case *proto.Publish:
			c.handlePublish(m)
		case *proto.PubRel:
			c.handlePubRel(m)
		case *proto.PubRec:
			c.handlePubRec(m)
		case *proto.PubComp:
			c.handlePubComp(m)
		case *proto.PingReq:
			c.submit(&proto.PingResp{})
		case *proto.Disconnect:
			c.handleDisconnect(m)
			c.Status = ClientDisconnectedNormally
			return
		case *proto.Subscribe:
			c.handleSubscribe(m)
		case *proto.Unsubscribe:
			c.handleUnsubscribe(m)
		default:
			log.Printf("reader: unknown msg type %T, continue anyway", m)
		}
		continue // loop until Disconnect comes.
	}
}

func (c *Connection) handleSubscribe(m *proto.Subscribe) {
	if m.Header.QosLevel != proto.QosAtLeastOnce {
		// protocol error, silent discarded(not disconnect)
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

		c.TopicList = append(c.TopicList, tq.Topic)
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
	for _, topic := range m.Topics {
		c.broker.Unsubscribe(topic, c)
	}
	ack := &proto.UnsubAck{MessageId: m.MessageId}
	c.submit(ack)
}

func (c *Connection) handleConnect(m *proto.Connect) {
	// Protocol check disabled due to difference between 3.1 and 3.1.1
	//	if m.ProtocolName != "MQIsdp" || // should be MQTT?
	//		m.ProtocolVersion != 3 {
	//		log.Print("reader: reject connection from ", m.ProtocolName, " version ", m.ProtocolVersion)
	//      connack := &proto.ConnAck{
	//		    ReturnCode: proto.RetCodeUnacceptableProtocolVersio,
	//	    }
	//		c.submit(connack)
	//		return
	//	}

	if m.UsernameFlag {
		if c.broker.Auth(m.Username, m.Password) == false {
			log.Printf("Auth failed: %s, %s", m.Username, c.conn.RemoteAddr())
			connack := &proto.ConnAck{
				ReturnCode: proto.RetCodeNotAuthorized,
			}
			c.submit(connack)
			return
		} else {
			c.Username = m.Username
		}

	}

	// Check client id.
	if len(m.ClientId) < 1 || len(m.ClientId) > 23 {
		connack := &proto.ConnAck{
			ReturnCode: proto.RetCodeIdentifierRejected,
		}
		c.submit(connack)
		return
	}
	c.clientid = m.ClientId

	clean := 0
	if m.CleanSession {
		clean = 1
	}

	currrent_c, err := c.storage.MergeClient(c.clientid, c, clean)
	if err != nil {
		c.storage.DeleteClient(c.clientid, c)
		return
	}

	if m.WillFlag {
		header := proto.Header{
			DupFlag:  false,
			QosLevel: m.WillQos,
			Retain:   m.WillRetain,
		}

		c.WillMsg = &proto.Publish{
			Header:    header,
			TopicName: m.WillTopic,
			Payload:   newStringPayload(m.WillMessage),
		}
	}

	connack := &proto.ConnAck{
		ReturnCode: proto.RetCodeAccepted,
	}
	currrent_c.submit(connack)

	log.Printf("New client connected from %v as %v (c%v, k%v).", currrent_c.conn.RemoteAddr(), currrent_c.clientid, clean, m.KeepAliveTimer)
}

func (c *Connection) handleDisconnect(m *proto.Disconnect) {
	for _, topic := range c.TopicList {
		c.broker.Unsubscribe(topic, c)
	}
	c.storage.DeleteClient(c.clientid, c)
	c.broker.stats.clientDisconnect()
}

func (c *Connection) handlePublish(m *proto.Publish) {
	c.broker.Publish(m)

	if m.Header.Retain {
		c.broker.UpdateRetain(m)
		log.Printf("Publish msg retained: %s", m.TopicName)
	}

	switch m.Header.QosLevel {
	case proto.QosAtLeastOnce:
		// do nothing
	case proto.QosAtMostOnce:
		c.submit(&proto.PubAck{MessageId: m.MessageId})
	case proto.QosExactlyOnce:
		c.submit(&proto.PubRec{MessageId: m.MessageId})
	default:
		log.Printf("Wrong QosLevel on Publish")
	}

	c.broker.stats.messageRecv()
}

func (c *Connection) handlePubRel(m *proto.PubRel) {
	c.submit(&proto.PubComp{MessageId: m.MessageId})
	log.Printf("PubComp sent")
}

func (c *Connection) handlePubRec(m *proto.PubRec) {
	c.submit(&proto.PubRel{MessageId: m.MessageId})
	log.Printf("PubRel sent")
}
func (c *Connection) handlePubComp(m *proto.PubComp) {
	// TODO:
}

// Queue a message; no notification of sending is done.
func (c *Connection) submit(m proto.Message) {
	storedMsgId := ""
	switch pubm := m.(type) {
	case *proto.Publish:
		storedMsgId = c.broker.storage.StoreMsg(c.clientid, pubm)
		log.Printf("msg stored: %s", storedMsgId)
		c.SendingMsgs.Put(storedMsgId)
	}

	log.Printf("%s, %d", c.clientid, c.Status)
	if c.Status != ClientAvailable {
		log.Printf("msg sent to not available client, msg stored: %s", c.clientid)
		return
	}

	j := job{m: m, storedmsgid: storedMsgId}
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
	defer func() {
		log.Printf("writer close: %s", c.clientid)
		c.conn.Close()
	}()

	for job := range c.jobs {
		log.Printf("writer begin: %T, %s", job.m, c.clientid)

		// Disconnect msg is used for shutdown writer goroutine.
		if _, ok := job.m.(*proto.Disconnect); ok {
			log.Print("writer: sent disconnect message")
			return
		}

		// TODO: write timeout
		err := job.m.Encode(c.conn)

		if err != nil {
			log.Print("writer: ", err)
			continue // Error does not shutdown Connection, wait re-connect
		}
		// if storedmsgid is set, (QoS 1 or 2,) move to sentQueue
		if job.storedmsgid != "" {
			c.SendingMsgs.Get() // TODO: it ssumes Queue is FIFO
			c.SentMsgs.Put(job.storedmsgid)
			log.Printf("msg %s is moved to SentMsgs", job.storedmsgid)
		}

		if job.r != nil {
			close(job.r)
		}
	}
}

func (c *Connection) Start() {
	go c.handleConnection()
	go c.writer()
}

func NewConnection(b *Broker, conn net.Conn) *Connection {
	c := &Connection{
		broker:      b,
		conn:        conn,
		storage:     b.storage,
		jobs:        make(chan job, b.conf.Queue.SendingQueueLength),
		Status:      ClientAvailable,
		LastUpdated: time.Now(),
		SendingMsgs: NewStoredQueue(b.conf.Queue.SendingQueueLength),
		SentMsgs:    NewStoredQueue(b.conf.Queue.SentQueueLength),
		//		out:      make(chan job, clientQueueLength),
		//		Incoming: make(chan *proto.Publish, clientQueueLength),
		//		done:     make(chan struct{}),
		//		connack:  make(chan *proto.ConnAck),
		//		suback:   make(chan *proto.SubAck),
	}
	return c
}

//
// StoredQueue is a fixed length queue to store messages in a connection.
//
// XXX: should be usecontainer/list ?

type storedQueueNode struct {
	storedMsgId string
	next        *storedQueueNode
}

type StoredQueue struct {
	head  *storedQueueNode
	tail  *storedQueueNode
	count int
	max   int
	lock  *sync.Mutex
}

func NewStoredQueue(max int) *StoredQueue {
	return &StoredQueue{
		lock: &sync.Mutex{},
		max:  max,
	}
}

func (q *storedQueueNode) Next() *storedQueueNode {
	return q.Next()
}

func (q *StoredQueue) Len() int {
	q.lock.Lock()
	defer q.lock.Unlock()

	return q.count
}

func (q *StoredQueue) Put(storedMsgId string) {
	q.lock.Lock()

	n := &storedQueueNode{storedMsgId: storedMsgId}

	if q.tail == nil {
		q.tail = n
		q.head = n
	} else {
		q.tail.next = n
		q.tail = n
	}
	q.count++

	if q.count > q.max {
		q.lock.Unlock()
		q.Get()
		return
	}
	q.lock.Unlock()
}
func (q *StoredQueue) Get() string {
	q.lock.Lock()
	defer q.lock.Unlock()

	n := q.head
	if n == nil {
		return ""
	}

	q.head = n.next

	if q.head == nil {
		q.tail = nil
	}
	q.count--

	return n.storedMsgId
}
