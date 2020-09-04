package gosocketio

import (
	"strconv"

	_ "time"

	"github.com/rmolak/golang-socketio/transport"
)

const (
	webSocketSchema       = "ws://"
	webSocketSecureSchema = "wss://"
	socketioWebsocketURL  = "/socket.io/?EIO=3&transport=websocket"

	pollingSchema       = "http://"
	pollingSecureSchema = "https://"
	socketioPollingURL  = "/socket.io/?EIO=3&transport=polling"
)

// Client represents socket.io client
type Client struct {
	*event
	*Channel
}

// AddrWebsocket returns an url for socket.io connection for websocket transport
func AddrWebsocket(host string, port int, secure bool) string {
	prefix := webSocketSchema
	if secure {
		prefix = webSocketSecureSchema
	}
	return prefix + host + ":" + strconv.Itoa(port) + socketioWebsocketURL
}

// AddrPolling returns an url for socket.io connection for polling transport
func AddrPolling(host string, port int, secure bool) string {
	prefix := pollingSchema
	if secure {
		prefix = pollingSecureSchema
	}
	return prefix + host + ":" + strconv.Itoa(port) + socketioPollingURL
}

// Dial connects to server and initializes socket.io protocol
// The correct ws protocol addr example:
// ws://myserver.com/socket.io/?EIO=3&transport=websocket
func Dial(addr string, tr transport.Transport) (*Client, error) {
	c := &Client{Channel: &Channel{}, event: &event{}}
	c.Channel.init()
	c.event.init()

	var err error
	c.conn, err = tr.Connect(addr)
	if err != nil {
		return nil, err
	}

	go c.Channel.inLoop(c.event)
	go c.Channel.outLoop(c.event)
	go c.Channel.pingLoop()

	switch tr.(type) {
	case *transport.PollingClientTransport:
		go c.event.callHandler(c.Channel, OnConnection)
	}

	return c, nil
}

// Close client connection
func (c *Client) Close() { c.Channel.close(c.event) }
