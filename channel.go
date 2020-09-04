package gosocketio

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/rmolak/golang-socketio/logging"
	"github.com/rmolak/golang-socketio/protocol"
	"github.com/rmolak/golang-socketio/transport"
)

const (
	queueBufferSize = 500
	headerForward   = "X-Forwarded-For"
)

var (
	ErrorSendTimeout     = errors.New("timeout")
	ErrorSocketOverflood = errors.New("socket overflood")
)

// connectionHeader represents engine.io connection header
type connectionHeader struct {
	Sid          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
}

// Channel represents socket.io connection
type Channel struct {
	conn transport.Connection

	outC       chan string
	stubC      chan string
	upgradedC  chan string
	connHeader connectionHeader

	alive   bool
	aliveMu sync.Mutex

	ack *acks

	server  *Server
	address string
	header  http.Header
}

// init the Channel
func (c *Channel) init() {
	c.outC, c.stubC, c.upgradedC = make(chan string, queueBufferSize), make(chan string), make(chan string)
	c.ack = &acks{}
	c.ack.ackC = make(map[int]chan string)
	c.alive = true
}

// Id returns an ID of the current socket connection
func (c *Channel) Id() string { return c.connHeader.Sid }

// IsAlive checks that Channel is still alive
func (c *Channel) IsAlive() bool {
	c.aliveMu.Lock()
	defer c.aliveMu.Unlock()
	return c.alive
}

// Close the client (Channel) connection
func (c *Channel) Close() error { return c.close(c.server.event) }

// stub closes the polling client (Channel) connection at socket.io upgrade
func (c *Channel) stub() error { return c.close(nil) }

// close channel
func (c *Channel) close(e *event) error {
	switch c.conn.(type) {
	case *transport.PollingConnection:
		logging.Log().Debug("Channel.close() type: PollingConnection")
	case *transport.WebsocketConnection:
		logging.Log().Debug("Channel.close() type: WebsocketConnection")
	}

	c.aliveMu.Lock()
	defer c.aliveMu.Unlock()

	if !c.alive { // already closed
		return nil
	}

	c.conn.Close()
	c.alive = false

	// clean outloop
	for len(c.outC) > 0 {
		<-c.outC
	}

	if e != nil { // close
		c.outC <- protocol.MessageClose
		e.callHandler(c, OnDisconnection)
	} else { // stub at transport upgrade
		c.outC <- protocol.MessageStub
	}

	overfloodedMu.Lock()
	delete(overflooded, c)
	overfloodedMu.Unlock()

	return nil
}

// inLoop is an incoming events loop
func (c *Channel) inLoop(e *event) error {
	for {
		message, err := c.conn.GetMessage()
		if err != nil {
			logging.Log().Debugf("Channel.inLoop(), c.conn.GetMessage() err: %v, message: %s", err, message)
			return c.close(e)
		}

		if message == transport.StopMessage {
			logging.Log().Debug("Channel.inLoop(): StopMessage")
			return nil
		}

		decodedMessage, err := protocol.Decode(message)
		if err != nil {
			logging.Log().Debugf("Channel.inLoop() decoding err: %v, message: %s", err, message)
			c.close(e)
			return err
		}

		switch decodedMessage.Type {
		case protocol.MessageTypeOpen:
			logging.Log().Debugf("Channel.inLoop(), protocol.MessageTypeOpen, decodedMessage: %+v", decodedMessage)
			if err := json.Unmarshal([]byte(decodedMessage.Source[1:]), &c.connHeader); err != nil {
				c.close(e)
			}
			e.callHandler(c, OnConnection)

		case protocol.MessageTypePing:
			logging.Log().Debugf("Channel.inLoop(), protocol.MessageTypePing, decodedMessage: %+v", decodedMessage)
			if decodedMessage.Source == protocol.MessagePingProbe {
				logging.Log().Debugf("Channel.inLoop(), decodedMessage.Source: %s", decodedMessage.Source)
				c.outC <- protocol.MessagePongProbe
				c.upgradedC <- transport.UpgradedMessage
			} else {
				c.outC <- protocol.MessagePong
			}

		case protocol.MessageTypeUpgrade:
		case protocol.MessageTypeBlank:
		case protocol.MessageTypePong:
		default:
			go e.processIncoming(c, decodedMessage)
		}
	}

	return nil
}

// outLoop is an outgoing events loop, sends messages from channel to socket
func (c *Channel) outLoop(e *event) error {
	for {
		outBufferLen := len(c.outC)
		logging.Log().Debug("Channel.outLoop(), outBufferLen:", outBufferLen)
		switch {
		case outBufferLen >= queueBufferSize-1:
			logging.Log().Debug("Channel.outLoop(), outBufferLen >= queueBufferSize-1")
			return c.close(e)
		case outBufferLen > int(queueBufferSize/2):
			overfloodedMu.Lock()
			overflooded[c] = struct{}{}
			overfloodedMu.Unlock()
		default:
			overfloodedMu.Lock()
			delete(overflooded, c)
			overfloodedMu.Unlock()
		}

		m := <-c.outC

		if m == protocol.MessageClose || m == protocol.MessageStub {
			return nil
		}

		if err := c.conn.WriteMessage(m); err != nil {
			logging.Log().Debug("Channel.outLoop(), failed to c.conn.WriteMessage() with err:", err)
			return c.close(e)
		}
	}
	return nil
}

// pingLoop sends ping messages for keeping connection alive
func (c *Channel) pingLoop() {
	for {
		interval, _ := c.conn.PingParams()
		time.Sleep(interval)
		if !c.IsAlive() {
			return
		}

		c.outC <- protocol.MessagePing
	}
}

// send message packet to the given channel c with payload
func (c *Channel) send(m *protocol.Message, payload interface{}) error {
	// preventing encoding/json "index out of range" panic
	defer func() {
		if r := recover(); r != nil {
			logging.Log().Warn("Channel.send(): recovered from panic:", r)
		}
	}()

	if payload != nil {
		b, err := json.Marshal(&payload)
		if err != nil {
			return err
		}
		m.Args = string(b)
	}

	command, err := protocol.Encode(m)
	if err != nil {
		return err
	}

	if len(c.outC) == queueBufferSize {
		return ErrorSocketOverflood
	}

	c.outC <- command
	return nil
}

// Emit an asynchronous event with the given name and payload
func (c *Channel) Emit(name string, payload interface{}) error {
	message := &protocol.Message{Type: protocol.MessageTypeEmit, EventName: name}
	return c.send(message, payload)
}

// Ack a synchronous event with the given name and payload and wait for/receive the response
func (c *Channel) Ack(name string, payload interface{}, timeout time.Duration) (string, error) {
	m := &protocol.Message{Type: protocol.MessageTypeAckRequest, AckID: c.ack.nextId(), EventName: name}

	ackC := make(chan string)
	c.ack.register(m.AckID, ackC)

	if err := c.send(m, payload); err != nil {
		c.ack.unregister(m.AckID)
	}

	select {
	case result := <-ackC:
		return result, nil
	case <-time.After(timeout):
		c.ack.unregister(m.AckID)
		return "", ErrorSendTimeout
	}
}

// IP returns an IP of the socket client
func (c *Channel) IP() string {
	forward := c.RequestHeader().Get(headerForward)
	if forward != "" {
		return forward
	}
	return c.address
}

// RequestHeader returns a connection request connectionHeader
func (c *Channel) RequestHeader() http.Header { return c.header }

// Join this channel to the given room
func (c *Channel) Join(room string) error {
	if c.server == nil {
		return ErrorServerNotSet
	}

	c.server.channelsMu.Lock()
	defer c.server.channelsMu.Unlock()

	if _, ok := c.server.channels[room]; !ok {
		c.server.channels[room] = make(map[*Channel]struct{})
	}

	if _, ok := c.server.rooms[c]; !ok {
		c.server.rooms[c] = make(map[string]struct{})
	}

	c.server.channels[room][c], c.server.rooms[c][room] = struct{}{}, struct{}{}
	return nil
}

// Leave the given room (remove channel from it)
func (c *Channel) Leave(room string) error {
	if c.server == nil {
		return ErrorServerNotSet
	}

	c.server.channelsMu.Lock()
	defer c.server.channelsMu.Unlock()

	if _, ok := c.server.channels[room]; ok {
		delete(c.server.channels[room], c)
		if len(c.server.channels[room]) == 0 {
			delete(c.server.channels, room)
		}
	}

	if _, ok := c.server.rooms[c]; ok {
		delete(c.server.rooms[c], room)
	}

	return nil
}

// Amount returns an amount of channels joined to the given room, using channel
func (c *Channel) Amount(room string) int {
	if c.server == nil {
		return 0
	}
	return c.server.Amount(room)
}

// List returns a list of channels joined to the given room, using channel
func (c *Channel) List(room string) []*Channel {
	if c.server == nil {
		return []*Channel{}
	}
	return c.server.List(room)
}

// BroadcastTo the the given room an event with given name and payload, using channel
func (c *Channel) BroadcastTo(room, name string, payload interface{}) {
	if c.server == nil {
		return
	}
	c.server.BroadcastTo(room, name, payload)
}
