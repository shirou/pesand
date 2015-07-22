package main

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	proto "github.com/huin/mqtt"
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
		clientLog := log.WithField("clientid", c.clientid)
		m, err := proto.DecodeOneMessage(c.conn, nil)
		if err != nil {

			clientLog.Warnf("disconnected unexpectedly: %s", err)

			if c.WillMsg != nil {
				clientLog.Info("Sending Will message")
				c.handlePublish(c.WillMsg)
			}

			c.Status = ClientUnAvailable
			return
		}
		clientLog.Infof("incoming msg type: %T", m)
		switch m := m.(type) {
		case *proto.Connect:
			c.handleConnect(m)
		case *proto.Publish:
			c.handlePublish(m)
		case *proto.PubAck:
			c.handlePubAck(m)
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
			clientLog.Warnf("unknown msg type %T, continue.", m)
		}
		continue // loop until Disconnect comes.
	}
}

//handleSubscribe
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
		c.broker.Subscribe(tq.Topic, c)
		suback.TopicsQos[i] = tq.Qos

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

//handleUnsubscribe
func (c *Connection) handleUnsubscribe(m *proto.Unsubscribe) {
	for _, topic := range m.Topics {
		c.broker.Unsubscribe(topic, c)
	}
	ack := &proto.UnsubAck{MessageId: m.MessageId}
	c.submit(ack)
}

//handleConnect
func (c *Connection) handleConnect(m *proto.Connect) {
	protoValidate := true
	switch m.ProtocolName {
	case "MQIsdp": // version 3.1
		if m.ProtocolVersion != 3 {
			protoValidate = false
		}
	case "MQTT": // version 3.1.1
		if m.ProtocolVersion != 4 {
			protoValidate = false
		}
	default:
		protoValidate = false
	}

	if protoValidate == false {

		log.WithFields(log.Fields{"name": m.ProtocolName, "version": m.ProtocolVersion}).
			Warn("reject connection")

		connack := &proto.ConnAck{
			ReturnCode: proto.RetCodeUnacceptableProtocolVersion,
		}
		c.submit(connack)
		return
	}

	if m.UsernameFlag {
		if c.broker.Auth(m.Username, m.Password) == false {

			log.WithFields(log.Fields{"username": m.Username, "addr": c.conn.RemoteAddr()}).
				Warn("Authorization Failed")

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

	log.WithFields(log.Fields{"clientid": currrent_c.clientid,
		"addr":      currrent_c.conn.RemoteAddr(),
		"clean":     clean,
		"keepAlive": m.KeepAliveTimer}).Info("New client connected")
}

//handleDisconnect
func (c *Connection) handleDisconnect(m *proto.Disconnect) {
	for _, topic := range c.TopicList {
		c.broker.Unsubscribe(topic, c)
	}
	c.storage.DeleteClient(c.clientid, c)
	c.broker.stats.clientDisconnect()
}

//handlePublish
func (c *Connection) handlePublish(m *proto.Publish) {
	c.broker.Publish(m)

	if m.Header.Retain {
		c.broker.UpdateRetain(m)
		log.WithField("topic", m.TopicName).Info("Publish msg retained")
	}

	log.WithFields(log.Fields{
		"QOS":     m.Header.QosLevel,
		"Payload": m.Payload,
		"MsgID":   m.MessageId}).Debug("msg recv body")

	switch m.Header.QosLevel {
	case proto.QosAtMostOnce:
		// do nothing
		break
	case proto.QosAtLeastOnce:
		c.submit(&proto.PubAck{MessageId: m.MessageId})
		log.WithField("MsgID", m.MessageId).Debug("QoS1: Puback sent")
		break
	case proto.QosExactlyOnce:
		c.submit(&proto.PubRec{MessageId: m.MessageId})
		log.WithField("MsgID", m.MessageId).Debug("QoS2: Pubrec sent")
		break
	default:
		log.WithField("QoS", m.Header.QosLevel).Warn("Wrong QosLevel on Publish")
		break
	}

	c.broker.stats.messageRecv()
}

//handlePubAck
func (c *Connection) handlePubAck(m *proto.PubAck) {
	// TODO:
	log.WithField("MsgID", m.MessageId).Debug("PubAck recieved")
}

//handlePubRel
func (c *Connection) handlePubRel(m *proto.PubRel) {
	// TODO:
	c.submit(&proto.PubComp{MessageId: m.MessageId})
	log.WithField("MsgID", m.MessageId).Debug("PubComp sent")
}

//handlePubRec
func (c *Connection) handlePubRec(m *proto.PubRec) {
	// TODO:
	c.submit(&proto.PubRel{MessageId: m.MessageId})
	log.WithField("MsgID", m.MessageId).Debug("PubRel sent")
}

//handlePubComp
func (c *Connection) handlePubComp(m *proto.PubComp) {
	// TODO:
	log.WithField("MsgID", m.MessageId).Debug("PubComp received")
}

//submit queues a message; no notification of sending is done.
func (c *Connection) submit(m proto.Message) {
	storedMsgId := ""
	switch pubm := m.(type) {
	case *proto.Publish:

		log.WithFields(log.Fields{
			"Payload": pubm.Payload,
			"Type":    fmt.Sprintf("%T", pubm.Payload),
			"MsgID":   pubm.MessageId}).Debugf("msg send body")

		if pubm.Header.QosLevel != proto.QosAtLeastOnce {
			storedMsgId = c.broker.storage.StoreMsg(c.clientid, pubm)

			log.WithField("MsgID", storedMsgId).Debug("msg stored")

			c.SendingMsgs.Put(storedMsgId)
		}
	}

	if c.Status != ClientAvailable {
		log.WithField("clientid", c.clientid).Info("msg sent to non-available client, msg stored")
		return
	}

	j := job{m: m, storedmsgid: storedMsgId}
	select {
	case c.jobs <- j:
	default:
		log.WithField("connection", c).Warn("failed to submit message")
	}
	return
}

//submitSync queues a message, returns a channel that will be readable
// when the message is sent.
func (c *Connection) submitSync(m proto.Message) receipt {
	j := job{m: m, r: make(receipt)}
	c.jobs <- j
	return j.r
}

//writer
func (c *Connection) writer() {
	defer func() {
		log.WithField("clientid", c.clientid).Info("writer close")
		c.conn.Close()
	}()

	for job := range c.jobs {

		log.WithFields(log.Fields{
			"clientID": c.clientid,
			"msgType":  job.m,
		}).Debug("sending msg")

		// Disconnect msg is used for shutdown writer goroutine.
		if _, ok := job.m.(*proto.Disconnect); ok {
			log.Warn("writer: sent disconnect message")
			return
		}

		// TODO: write timeout
		err := job.m.Encode(c.conn)

		if err != nil {
			log.Warnf("writer error: ", err)
			continue // Error does not shutdown Connection, wait re-connect
		}
		// if storedmsgid is set, (QoS 1 or 2) move to sentQueue
		if job.storedmsgid != "" {
			c.SendingMsgs.Get() // TODO: it assumes Queue is FIFO
			c.SentMsgs.Put(job.storedmsgid)

			log.WithField("msgID", job.storedmsgid).Debug("msg moved to SentMsgs")
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
// XXX: should be use container/list ?
// http://bl.ocks.org/dz1984/6963545

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
