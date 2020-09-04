package transport

import (
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"fmt"
	"github.com/rmolak/golang-socketio/logging"
	"github.com/rmolak/golang-socketio/protocol"
)

const (
	PlDefaultPingInterval   = 30 * time.Second
	PlDefaultPingTimeout    = 60 * time.Second
	PlDefaultReceiveTimeout = 60 * time.Second
	PlDefaultSendTimeout    = 60 * time.Second

	StopMessage     = "stop"
	UpgradedMessage = "upgrade"
	noError         = "0"

	hijackingNotSupported = "webserver doesn't support hijacking"
)

var (
	errGetMessageTimeout       = errors.New("timeout waiting for the message")
	errReceivedConnectionClose = errors.New("received connection close")
	errWriteMessageTimeout     = errors.New("timeout waiting for write")
)

// withLength returns s as a message with length
func withLength(m string) string { return fmt.Sprintf("%d:%s", len(m), m) }

// PollingTransportParams represents XHR polling transport params
type PollingTransportParams struct {
	Headers http.Header
}

// PollingConnection represents a XHR polling connection
type PollingConnection struct {
	Transport  *PollingTransport
	eventsInC  chan string
	eventsOutC chan string
	errors     chan string
	sessionID  string
}

// GetMessage waits for incoming message from the connection
func (polling *PollingConnection) GetMessage() (string, error) {
	select {
	case <-time.After(polling.Transport.ReceiveTimeout):
		logging.Log().Debug("PollingConnection.GetMessage() timed out")
		return "", errGetMessageTimeout
	case m := <-polling.eventsInC:
		logging.Log().Debug("PollingConnection.GetMessage() received:", m)
		if m == protocol.MessageClose {
			logging.Log().Debug("PollingConnection.GetMessage() received connection close")
			return "", errReceivedConnectionClose
		}
		return m, nil
	}
}

// WriteMessage to the connection
func (polling *PollingConnection) WriteMessage(message string) error {
	logging.Log().Debug("PollingConnection.WriteMessage() fired with:", message)
	polling.eventsOutC <- message
	logging.Log().Debug("PollingConnection.WriteMessage() written to eventsOutC:", message)
	select {
	case <-time.After(polling.Transport.SendTimeout):
		return errWriteMessageTimeout
	case errString := <-polling.errors:
		if errString != noError {
			logging.Log().Debug("PollingConnection.WriteMessage() failed to write with err:", errString)
			return errors.New(errString)
		}
	}
	return nil
}

// Close the polling connection and delete session
func (polling *PollingConnection) Close() error {
	logging.Log().Debug("PollingConnection.Close() fired for session:", polling.sessionID)
	err := polling.WriteMessage(protocol.MessageBlank)
	polling.Transport.sessions.Delete(polling.sessionID)
	return err
}

// PingParams returns a connection ping params
func (polling *PollingConnection) PingParams() (time.Duration, time.Duration) {
	return polling.Transport.PingInterval, polling.Transport.PingTimeout
}

// sessions describes sessions needed for identifying polling connections with socket.io connections
type sessions struct {
	sync.Mutex
	m map[string]*PollingConnection
}

// Set sets sessionID to the given connection
func (s *sessions) Set(sessionID string, conn *PollingConnection) {
	logging.Log().Debug("sessions.Set() fired with:", sessionID)
	s.Lock()
	defer s.Unlock()
	s.m[sessionID] = conn
}

// Delete the sessionID
func (s *sessions) Delete(sessionID string) {
	logging.Log().Debug("sessions.Delete() fired with:", sessionID)
	s.Lock()
	defer s.Unlock()
	delete(s.m, sessionID)
}

// Get returns polling connection if it exists, otherwise returns nil
func (s *sessions) Get(sessionID string) *PollingConnection {
	s.Lock()
	defer s.Unlock()
	return s.m[sessionID]
}

// PollingTransport represens the XHR polling transport params
type PollingTransport struct {
	PingInterval   time.Duration
	PingTimeout    time.Duration
	ReceiveTimeout time.Duration
	SendTimeout    time.Duration

	Headers  http.Header
	sessions sessions
}

// Connect for the polling transport is a placeholder
func (t *PollingTransport) Connect(url string) (Connection, error) {
	return nil, nil
}

// HandleConnection returns a pointer to a new Connection
func (t *PollingTransport) HandleConnection(w http.ResponseWriter, r *http.Request) (Connection, error) {
	return &PollingConnection{
		Transport:  t,
		eventsInC:  make(chan string),
		eventsOutC: make(chan string),
		errors:     make(chan string),
	}, nil
}

// SetSid to the given sessionID and connection
func (t *PollingTransport) SetSid(sessionID string, connection Connection) {
	t.sessions.Set(sessionID, connection.(*PollingConnection))
	connection.(*PollingConnection).sessionID = sessionID
}

// Serve is for receiving messages from client, simple decoding also here
func (t *PollingTransport) Serve(w http.ResponseWriter, r *http.Request) {
	sessionId := r.URL.Query().Get("sid")
	conn := t.sessions.Get(sessionId)
	if conn == nil {
		return
	}

	switch r.Method {
	case http.MethodGet:
		logging.Log().Debug("PollingTransport.Serve() is serving GET request")
		conn.PollingWriter(w, r)
	case http.MethodPost:
		bodyBytes, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			logging.Log().Debug("PollingTransport.Serve() error ioutil.ReadAll():", err)
			return
		}

		bodyString := string(bodyBytes)
		logging.Log().Debug("PollingTransport.Serve() POST bodyString before split:", bodyString)
		index := strings.Index(bodyString, ":")
		body := bodyString[index+1:]

		setHeaders(w)

		logging.Log().Debug("PollingTransport.Serve() POST body:", body)
		w.Write([]byte("ok"))
		logging.Log().Debug("PollingTransport.Serve() written POST response")
		conn.eventsInC <- body
		logging.Log().Debug("PollingTransport.Serve() sent to eventsInC")
	}
}

// DefaultPollingTransport returns PollingTransport with default params
func DefaultPollingTransport() *PollingTransport {
	return &PollingTransport{
		PingInterval:   PlDefaultPingInterval,
		PingTimeout:    PlDefaultPingTimeout,
		ReceiveTimeout: PlDefaultReceiveTimeout,
		SendTimeout:    PlDefaultSendTimeout,
		sessions: sessions{
			Mutex: sync.Mutex{},
			m:     map[string]*PollingConnection{},
		},
		Headers: nil,
	}
}

// PollingWriter for writing polling answer
func (polling *PollingConnection) PollingWriter(w http.ResponseWriter, r *http.Request) {
	setHeaders(w)
	select {
	case <-time.After(polling.Transport.SendTimeout):
		logging.Log().Debug("PollingTransport.PollingWriter() timed out")
		polling.errors <- noError
	case message := <-polling.eventsOutC:
		logging.Log().Debug("PollingTransport.PollingWriter() prepares to write message:", message)
		message = withLength(message)
		if message == withLength(protocol.MessageBlank) {
			logging.Log().Debug("PollingTransport.PollingWriter() writing 1:6")

			hj, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, hijackingNotSupported, http.StatusInternalServerError)
				return
			}

			conn, buffer, err := hj.Hijack()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			defer conn.Close()

			buffer.WriteString("HTTP/1.1 200 OK\r\n" +
				"Cache-Control: no-cache, private\r\n" +
				"Content-Length: 3\r\n" +
				"Date: Mon, 24 Nov 2016 10:21:21 GMT\r\n\r\n")
			buffer.WriteString(withLength(protocol.MessageBlank))
			buffer.Flush()
			logging.Log().Debug("PollingTransport.PollingWriter() hijack returns")
			polling.errors <- noError
			polling.eventsInC <- StopMessage
		} else {
			_, err := w.Write([]byte(message))
			logging.Log().Debug("PollingTransport.PollingWriter() written message:", message)
			if err != nil {
				logging.Log().Debug("PollingTransport.PollingWriter() failed to write message with err:", err)
				polling.errors <- err.Error()
				return
			}
			polling.errors <- noError
		}
	}
}

// setHeaders into w
func setHeaders(w http.ResponseWriter) {
	// We are going to return JSON no matter what:
	w.Header().Set("Content-Type", "application/json")
	// Don't cache response:
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate") // HTTP 1.1
	w.Header().Set("Pragma", "no-cache")                                   // HTTP 1.0
	w.Header().Set("Expires", "0")                                         // Proxies
}
