package gosocketio

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/rmolak/golang-socketio/logging"
	"github.com/rmolak/golang-socketio/protocol"
	"github.com/rmolak/golang-socketio/transport"
)

var (
	ErrorServerNotSet       = errors.New("server was not set")
	ErrorConnectionNotFound = errors.New("connection not found")
)

// Server represents a socket.io server instance
type Server struct {
	*event
	http.Handler

	channels   map[string]map[*Channel]struct{} // maps room name to map of channels to an empty struct
	rooms      map[*Channel]map[string]struct{} // maps channel to map of room names to an empty struct
	channelsMu sync.RWMutex

	sids   map[string]*Channel // maps channel id to channel
	sidsMu sync.RWMutex

	websocket *transport.WebsocketTransport
	polling   *transport.PollingTransport
}

// NewServer creates new socket.io server
func NewServer() *Server {
	s := &Server{
		websocket: transport.DefaultWebsocketTransport(),
		polling:   transport.DefaultPollingTransport(),
		channels:  make(map[string]map[*Channel]struct{}),
		rooms:     make(map[*Channel]map[string]struct{}),
		sids:      make(map[string]*Channel),
		event: &event{
			onConnection:    onConnection,
			onDisconnection: onDisconnection,
		},
	}
	s.event.init()
	return s
}

// GetChannel by it's sid
func (s *Server) GetChannel(sid string) (*Channel, error) {
	s.sidsMu.RLock()
	defer s.sidsMu.RUnlock()

	c, ok := s.sids[sid]
	if !ok {
		return nil, ErrorConnectionNotFound
	}

	return c, nil
}

// Get amount of channels, joined to given room, using server
func (s *Server) Amount(room string) int {
	s.channelsMu.RLock()
	defer s.channelsMu.RUnlock()
	roomChannels, _ := s.channels[room]
	return len(roomChannels)
}

// List returns a list of channels joined to the given room, using server
func (s *Server) List(room string) []*Channel {
	s.channelsMu.RLock()
	defer s.channelsMu.RUnlock()

	roomChannels, ok := s.channels[room]
	if !ok {
		return []*Channel{}
	}

	i := 0
	roomChannelsCopy := make([]*Channel, len(roomChannels))
	for channel := range roomChannels {
		roomChannelsCopy[i] = channel
		i++
	}

	return roomChannelsCopy
}

// BroadcastTo the the given room an handler with payload, using server
func (s *Server) BroadcastTo(room, name string, payload interface{}) {
	s.channelsMu.RLock()
	defer s.channelsMu.RUnlock()

	roomChannels, ok := s.channels[room]
	if !ok {
		return
	}

	for cn := range roomChannels {
		if cn.IsAlive() {
			go cn.Emit(name, payload)
		}
	}
}

// Broadcast to all clients
func (s *Server) BroadcastToAll(method string, payload interface{}) {
	s.sidsMu.RLock()
	defer s.sidsMu.RUnlock()

	for _, cn := range s.sids {
		if cn.IsAlive() {
			go cn.Emit(method, payload)
		}
	}
}

// onConnection fires on connection and on connection upgrade
func onConnection(c *Channel) {
	c.server.sidsMu.Lock()
	c.server.sids[c.Id()] = c
	c.server.sidsMu.Unlock()
}

// onDisconnection fires on disconnection
func onDisconnection(c *Channel) {
	c.server.channelsMu.Lock()
	defer c.server.channelsMu.Unlock()

	defer func() {
		c.server.sidsMu.Lock()
		delete(c.server.sids, c.Id())
		c.server.sidsMu.Unlock()
	}()

	_, ok := c.server.rooms[c]
	if !ok {
		return
	}

	for room := range c.server.rooms[c] {
		if curRoom, ok := c.server.channels[room]; ok {
			delete(curRoom, c)
			if len(curRoom) == 0 {
				delete(c.server.channels, room)
			}
		}
	}
	delete(c.server.rooms, c)
}

// sendOpenSequence to the given channel c
func (s *Server) sendOpenSequence(c *Channel) {
	jsonHdr, err := json.Marshal(&c.connHeader)
	if err != nil {
		panic(err)
	}
	c.outC <- protocol.MustEncode(&protocol.Message{Type: protocol.MessageTypeOpen, Args: string(jsonHdr)})
	c.outC <- protocol.MustEncode(&protocol.Message{Type: protocol.MessageTypeEmpty})
}

// setupEventLoop for the given connection conn on the given address with HTTP header
func (s *Server) setupEventLoop(conn transport.Connection, address string, header http.Header) {
	interval, timeout := conn.PingParams()
	connHeader := connectionHeader{
		Sid: func(s string) string {
			hash := fmt.Sprintf("%s %s %b %b", s, time.Now(), rand.Uint32(), rand.Uint32())
			buf, sum := bytes.NewBuffer(nil), md5.Sum([]byte(hash))
			encoder := base64.NewEncoder(base64.URLEncoding, buf)
			encoder.Write(sum[:])
			encoder.Close()
			return buf.String()[:20]
		}(address),
		Upgrades:     []string{"websocket"},
		PingInterval: int(interval / time.Millisecond),
		PingTimeout:  int(timeout / time.Millisecond),
	}

	c := &Channel{conn: conn, address: address, header: header, server: s, connHeader: connHeader}
	c.init()

	switch conn.(type) {
	case *transport.PollingConnection:
		conn.(*transport.PollingConnection).Transport.SetSid(connHeader.Sid, conn)
	}

	s.sendOpenSequence(c)

	go c.inLoop(s.event)
	go c.outLoop(s.event)

	s.callHandler(c, OnConnection)
}

// upgradeEventLoop at transport upgrade
func (s *Server) upgradeEventLoop(conn transport.Connection, remoteAddr string, header http.Header, sid string) {
	logging.Log().Debug("Server.upgradeEventLoop() fired")

	pollingChannel, err := s.GetChannel(sid)
	if err != nil {
		logging.Log().Warn("Server.upgradeEventLoop() can't find channel for session:", sid)
		return
	}

	logging.Log().Debug("Server.upgradeEventLoop() obtained a polling channel")
	interval, timeout := conn.PingParams()
	connHeader := connectionHeader{
		Sid:          sid,
		Upgrades:     []string{},
		PingInterval: int(interval / time.Millisecond),
		PingTimeout:  int(timeout / time.Millisecond),
	}

	c := &Channel{conn: conn, address: remoteAddr, header: header, server: s, connHeader: connHeader}
	c.init()
	logging.Log().Debug("Server.upgradeEventLoop() initialized a new channel")

	go c.inLoop(s.event)
	go c.outLoop(s.event)

	logging.Log().Debug("Server.upgradeEventLoop() fired c.inLoop() and c.outLoop() in separate go-routines")
	onConnection(c)

	// synchronize stubbing polling channel with receiving "2probe" message
	<-c.upgradedC
	pollingChannel.stub()
}

// ServeHTTP makes Server to implement http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	session, transportName := r.URL.Query().Get("sid"), r.URL.Query().Get("transport")

	switch transportName {
	case "polling":
		// session is empty in first polling request, or first and single websocket request
		if session != "" {
			s.polling.Serve(w, r)
			return
		}

		conn, err := s.polling.HandleConnection(w, r)
		if err != nil {
			return
		}

		s.setupEventLoop(conn, r.RemoteAddr, r.Header)
		logging.Log().Debug("Server.ServeHTTP() created a PollingConnection")
		conn.(*transport.PollingConnection).PollingWriter(w, r)

	case "websocket":
		if session != "" {
			logging.Log().Debug("Server.ServeHTTP() is firing s.websocket.HandleConnection() for upgrade")
			conn, err := s.websocket.HandleConnection(w, r)
			if err != nil {
				logging.Log().Debug("Server.ServeHTTP() upgrade error:", err)
				return
			}
			s.upgradeEventLoop(conn, r.RemoteAddr, r.Header, session)
			logging.Log().Debug("Server.ServeHTTP() upgraded to a WebsocketConnection")
			return
		}

		conn, err := s.websocket.HandleConnection(w, r)
		if err != nil {
			return
		}

		s.setupEventLoop(conn, r.RemoteAddr, r.Header)
		logging.Log().Debug("Server.ServeHTTP() created a WebsocketConnection")
	}
}

// CountChannels returns an amount of connected channels
func (s *Server) CountChannels() int {
	s.sidsMu.RLock()
	defer s.sidsMu.RUnlock()
	return len(s.sids)
}

// CountRooms returns an amount of rooms with at least one joined channel
func (s *Server) CountRooms() int {
	s.channelsMu.RLock()
	defer s.channelsMu.RUnlock()
	return len(s.channels)
}
