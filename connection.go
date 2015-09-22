package main

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	proto "github.com/huin/mqtt"
	"net"
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

type Session struct {
	Clientid    string
	Status      uint8
	TopicList   []string // Subscribed topic list
	LastUpdated time.Time
	SendingMsgs *StoredQueue // msgs which not sent
	SentMsgs    *StoredQueue // msgs which already sent
	WillMsg     *proto.Publish
	Username    string
}

type Connection struct {
	Conn              net.Conn
	broker            *Broker
	jobs              chan job
	KeepAliveTimer    uint16
	lastKeepAliveTime time.Time
	sess              *Session
	//Done              chan struct{}
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
		c.Conn.Close()
		close(c.jobs)
	}()

	for {
		clientLog := log.WithField("clientid", c.sess.Clientid)
		m, err := proto.DecodeOneMessage(c.Conn, nil)
		if err != nil {

			clientLog.Warnf("disconnected unexpectedly: %s", err)

			if c.sess.WillMsg != nil {
				clientLog.Info("Sending Will message")
				c.handlePublish(c.sess.WillMsg)
			}

			c.sess.Status = ClientUnAvailable
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
			c.sess.Status = ClientDisconnectedNormally
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

		c.broker.Subscribe(tq.Topic, c.sess)
		suback.TopicsQos[i] = tq.Qos

		c.sess.TopicList = append(c.sess.TopicList, tq.Topic)
	}
	c.submit(suback)

	// Process retained messages.
	for _, tq := range m.Topics {
		pubmsg, err := c.broker.storage.GetRetain(tq.Topic)
		if err != nil {
			log.WithField("topic", tq).
				Error(err)
			continue
		}
		c.submit(pubmsg)

	}
}

//handleUnsubscribe
func (c *Connection) handleUnsubscribe(m *proto.Unsubscribe) {
	for _, topic := range m.Topics {
		c.broker.Unsubscribe(topic, c.sess)
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

	//authorize connection
	if ok := c.authorizeConn(m); !ok {
		log.WithFields(log.Fields{"username": m.Username, "addr": c.Conn.RemoteAddr()}).
			Warn("Authorization Failed")

		connack := &proto.ConnAck{
			ReturnCode: proto.RetCodeNotAuthorized,
		}
		c.submit(connack)
		return
	}

	// Check client id.
	if len(m.ClientId) < 1 || len(m.ClientId) > 23 {
		connack := &proto.ConnAck{
			ReturnCode: proto.RetCodeIdentifierRejected,
		}
		c.submit(connack)
		return
	}
	c.sess.Clientid = m.ClientId

	clean := 0
	if m.CleanSession {
		clean = 1
	}

	var err error
	c.sess, err = c.broker.storage.MergeClientSession(c.sess.Clientid, c.sess, clean)
	if err != nil {
		log.WithFields(log.Fields{"clientid": c.sess.Clientid,
			"clean":     clean,
			"keepAlive": m.KeepAliveTimer}).Errorf("MergeSession Error %s", err)
		c.broker.storage.DeleteClientSession(c.sess.Clientid)
		if err != nil {
			log.WithField("Clientid", c.sess.Clientid).Errorf("DeleteClientSession %v", err)
		}
		return
	}

	//seems like a bad idea?
	log.Printf("save connection %s", c.sess.Clientid)
	c.broker.clientsMu.Lock()
	c.broker.clients[c.sess.Clientid] = c
	c.broker.clientsMu.Unlock()

	if m.WillFlag {
		header := proto.Header{
			DupFlag:  false,
			QosLevel: m.WillQos,
			Retain:   m.WillRetain,
		}

		c.sess.WillMsg = &proto.Publish{
			Header:    header,
			TopicName: m.WillTopic,
			Payload:   newStringPayload(m.WillMessage),
		}
	}

	connack := &proto.ConnAck{
		ReturnCode: proto.RetCodeAccepted,
	}
	c.submit(connack)

	log.WithFields(log.Fields{"clientid": c.sess.Clientid,
		"addr":      c.Conn.RemoteAddr(),
		"clean":     clean,
		"keepAlive": m.KeepAliveTimer}).Info("New client connected")
}

//handleDisconnect
func (c *Connection) handleDisconnect(m *proto.Disconnect) {
	for _, topic := range c.sess.TopicList {
		c.broker.Unsubscribe(topic, c.sess)
	}
	err := c.broker.storage.DeleteClientSession(c.sess.Clientid)
	if err != nil {
		log.WithField("Clientid", c.sess.Clientid).Errorf("DeleteClientSession %v", err)
	}
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
			storedMsgId, err := c.broker.storage.StoreMsg(c.sess.Clientid, pubm)
			if err != nil {
				log.Error(err)
				return
			}

			log.WithField("MsgID", storedMsgId).Debug("msg stored")

			c.sess.SendingMsgs.Push(storedMsgId)
		}
	}

	if c.sess.Status != ClientAvailable {
		log.WithField("clientid", c.sess.Clientid).Info("msg sent to non-available client, msg stored")
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
		log.WithField("clientid", c.sess.Clientid).Info("writer close")
		c.Conn.Close()
	}()

	for job := range c.jobs {

		log.WithFields(log.Fields{
			"clientID": c.sess.Clientid,
			"msgType":  job.m,
		}).Debug("sending msg")

		// Disconnect msg is used for shutdown writer goroutine.
		if _, ok := job.m.(*proto.Disconnect); ok {
			log.Warn("writer: sent disconnect message")
			return
		}

		// TODO: write timeout
		err := job.m.Encode(c.Conn)

		if err != nil {
			log.Warnf("writer error: ", err)
			continue // Error does not shutdown Connection, wait re-connect
		}
		// if storedmsgid is set, (QoS 1 or 2) move to sentQueue
		if job.storedmsgid != "" {
			c.sess.SendingMsgs.Pop() // TODO: it assumes Queue is FIFO
			c.sess.SentMsgs.Push(job.storedmsgid)

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

//authorizeConn
func (c *Connection) authorizeConn(m *proto.Connect) bool {

	if !c.broker.AllowAnon() { //authorization required

		if !m.UsernameFlag { //if no user name flag
			return false
		}

		if !c.broker.Auth(m.Username, m.Password) { //if authorization failed
			return false
		}

		c.sess.Username = m.Username
		return true //auth success

	} else { //authorization not required

		if m.UsernameFlag { //optionally you can still authorize
			if c.broker.Auth(m.Username, m.Password) == false {
				return false //authoization attempt failed
			} else {
				c.sess.Username = m.Username //auth success
				return true
			}
		}
		return true //anonymous connection

	}
}

//NewConnection
func NewConnection(b *Broker, conn net.Conn) *Connection {
	c := &Connection{
		broker: b,
		Conn:   conn,
		jobs:   make(chan job, b.conf.Queue.SendingQueueLength),
		sess: &Session{
			Status:      ClientAvailable,
			LastUpdated: time.Now(),
			SendingMsgs: NewStoredQueue(), //NewStoredQueue(b.conf.Queue.SendingQueueLength),
			SentMsgs:    NewStoredQueue(), //NewStoredQueue(b.conf.Queue.SentQueueLength),
			//		WillMsg:     &proto.Publish{},
			//		out:      make(chan job, clientQueueLength),
			//		Incoming: make(chan *proto.Publish, clientQueueLength),
			//		done:     make(chan struct{}),
			//		connack:  make(chan *proto.ConnAck),
			//		suback:   make(chan *proto.SubAck),
		},
	}
	return c
}
