package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/rmolak/golang-socketio/logging"
	"github.com/rmolak/golang-socketio/protocol"
)

var (
	errResponseIsNotOK       = errors.New("response body is not OK")
	errAnswerNotOpenSequence = errors.New("not opensequence answer")
	errAnswerNotOpenMessage  = errors.New("not openmessage answer")
)

// PollingClientConnection represents XHR polling client connection
type PollingClientConnection struct {
	transport *PollingClientTransport
	client    *http.Client
	url       string
	sid       string
}

// GetMessage performs a GET request to wait for the following message
func (polling *PollingClientConnection) GetMessage() (string, error) {
	logging.Log().Debug("PollingConnection.GetMessage() fired")

	resp, err := polling.client.Get(polling.url)
	if err != nil {
		logging.Log().Debug("PollingConnection.GetMessage() error polling.client.Get():", err)
		return "", err
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logging.Log().Debug("PollingConnection.GetMessage() error ioutil.ReadAll():", err)
		return "", err
	}

	bodyString := string(bodyBytes)
	logging.Log().Debug("PollingConnection.GetMessage() bodyString:", bodyString)
	index := strings.Index(bodyString, ":")

	body := bodyString[index+1:]
	return body, nil
}

// WriteMessage performs a POST request to send a message to server
func (polling *PollingClientConnection) WriteMessage(m string) error {
	mWrite := withLength(m)
	logging.Log().Debug("PollingConnection.WriteMessage() fired, msgToWrite:", mWrite)
	mJSON := []byte(mWrite)

	resp, err := polling.client.Post(polling.url, "application/json", bytes.NewBuffer(mJSON))
	if err != nil {
		logging.Log().Debug("PollingConnection.WriteMessage() error polling.client.Post():", err)
		return err
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logging.Log().Debug("PollingConnection.WriteMessage() error ioutil.ReadAll():", err)
		return err
	}

	resp.Body.Close()
	bodyString := string(bodyBytes)
	if bodyString != "ok" {
		return errResponseIsNotOK
	}

	return nil
}

// Close the client connection gracefully
func (polling *PollingClientConnection) Close() error {
	return polling.WriteMessage(protocol.MessageClose)
}

// PingParams returns PingInterval and PingTimeout params
func (polling *PollingClientConnection) PingParams() (time.Duration, time.Duration) {
	return polling.transport.PingInterval, polling.transport.PingTimeout
}

// PollingClientTransport represents polling client transport parameters
type PollingClientTransport struct {
	PingInterval   time.Duration
	PingTimeout    time.Duration
	ReceiveTimeout time.Duration
	SendTimeout    time.Duration

	Headers  http.Header
	sessions sessions
}

// HandleConnection for the polling client is a placeholder
func (t *PollingClientTransport) HandleConnection(w http.ResponseWriter, r *http.Request) (Connection, error) {
	return nil, nil
}

// Serve for the polling client is a placeholder
func (t *PollingClientTransport) Serve(w http.ResponseWriter, r *http.Request) {}

// SetSid for the polling client is a placeholder
func (t *PollingClientTransport) SetSid(sid string, conn Connection) {}

// openSequence represents a connection open sequence parameters
type openSequence struct {
	Sid          string        `json:"sid"`
	Upgrades     []string      `json:"upgrades"`
	PingInterval time.Duration `json:"pingInterval"`
	PingTimeout  time.Duration `json:"pingTimeout"`
}

// Connect to server, perform 3 HTTP requests in connecting sequence
func (t *PollingClientTransport) Connect(url string) (Connection, error) {
	polling := &PollingClientConnection{transport: t, client: &http.Client{}, url: url}

	resp, err := polling.client.Get(polling.url)
	if err != nil {
		logging.Log().Debug("PollingConnection.Connect() error polling.client.Get() 1:", err)
		return nil, err
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logging.Log().Debug("PollingConnection.Connect() error ioutil.ReadAll() 1:", err)
		return nil, err
	}

	resp.Body.Close()
	bodyString := string(bodyBytes)
	logging.Log().Debug("PollingConnection.Connect() bodyString 1:", bodyString)

	body := bodyString[strings.Index(bodyString, ":")+1:]
	if string(body[0]) != protocol.MessageOpen {
		return nil, errAnswerNotOpenSequence
	}

	bodyBytes2 := []byte(body[1:])
	var openSequence openSequence

	if err := json.Unmarshal(bodyBytes2, &openSequence); err != nil {
		logging.Log().Debug("PollingConnection.Connect() error json.Unmarshal() 1:", err)
		return nil, err
	}

	polling.url += "&sid=" + openSequence.Sid
	logging.Log().Debug("PollingConnection.Connect() polling.url 1:", polling.url)

	resp, err = polling.client.Get(polling.url)
	if err != nil {
		logging.Log().Debug("PollingConnection.Connect() error plc.client.Get() 2:", err)
		return nil, err
	}

	bodyBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		logging.Log().Debug("PollingConnection.Connect() error ioutil.ReadAll() 2:", err)
		return nil, err
	}

	resp.Body.Close()
	bodyString = string(bodyBytes)
	logging.Log().Debug("PollingConnection.Connect() bodyString 2:", bodyString)
	body = bodyString[strings.Index(bodyString, ":")+1:]

	if body != protocol.MessageEmpty {
		return nil, errAnswerNotOpenMessage
	}

	return polling, nil
}

// DefaultPollingClientTransport returns client polling transport with default params
func DefaultPollingClientTransport() *PollingClientTransport {
	return &PollingClientTransport{
		PingInterval:   PlDefaultPingInterval,
		PingTimeout:    PlDefaultPingTimeout,
		ReceiveTimeout: PlDefaultReceiveTimeout,
		SendTimeout:    PlDefaultSendTimeout,
	}
}
