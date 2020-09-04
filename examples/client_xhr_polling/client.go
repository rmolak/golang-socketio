package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/rmolak/golang-socketio"
	"github.com/rmolak/golang-socketio/examples/model"
	"github.com/rmolak/golang-socketio/transport"
)

const (
	serverEventJoin  = "join"
	serverEventSend  = "send"
	serverEventLeave = "leave"

	someEventName = "someEvent"
)

const defaultTimeout = time.Second * 30

const serverPort = 3811

func onConnectionHandler(c *gosocketio.Channel)    { log.Printf("Connected %s\n", c.Id()) }
func onDisconnectionHandler(c *gosocketio.Channel) { log.Printf("Disconnected %s\n", c.Id()) }

// SomeEventPayload represents a payload for event someEvent
type SomeEventPayload struct {
	IntField    int    `json:"i"`
	StringField string `json:"s"`
}

var sendResultC = make(chan SomeEventPayload)

func onSomeEventHandler(c *gosocketio.Channel, data interface{}) {
	log.Printf("--- Client channel %s received someEvent with data: %v\n", c.Id(), data)
	j, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}
	var obj SomeEventPayload
	if err := json.Unmarshal(j, &obj); err != nil {
		log.Fatal(err)
	}

	sendResultC <- obj
	log.Printf("--- callback send data to channel")
}

func main() {
	client, err := gosocketio.Dial(
		gosocketio.AddrPolling("localhost", serverPort, false),
		transport.DefaultPollingClientTransport(),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.On(gosocketio.OnConnection, onConnectionHandler); err != nil {
		log.Fatal(err)
	}
	if err := client.On(gosocketio.OnDisconnection, onDisconnectionHandler); err != nil {
		log.Fatal(err)
	}

	if err := client.On(someEventName, onSomeEventHandler); err != nil {
		log.Fatal(err)
	}

	const roomName = "room1"

	// join the room
	joinResult, err := client.Ack(serverEventJoin, roomName, defaultTimeout)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("join result:", joinResult)

	// make server to send back payload with Emit()
	log.Println("awaiting payload back via server's Emit()...")
	payloadToSend := model.Data{
		EventName: someEventName,
		Payload:   SomeEventPayload{7, "seven"},
	}
	sendResult, err := client.Ack(serverEventSend, payloadToSend, defaultTimeout)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("send result:", sendResult)

	resultingPayload := <-sendResultC
	log.Println("sent payload:", payloadToSend.Payload)
	log.Println("resulting emitted payload:", resultingPayload)

	// make server to send back payload with BroadcastTo()
	log.Println("awaiting payload back via server's BroadcastTo()...")
	payloadToSend = model.Data{
		EventName:         someEventName,
		BroadcastRoomName: roomName,
		Payload:           SomeEventPayload{8, "eight"},
	}
	sendResult, err = client.Ack(serverEventSend, payloadToSend, defaultTimeout)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("send result:", sendResult)

	resultingPayload = <-sendResultC
	log.Println("resulting broadcasted payload:", resultingPayload)

	leaveResult, err := client.Ack(serverEventLeave, roomName, defaultTimeout)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("leave result:", leaveResult)

	client.Close()
	log.Println("Client closed")
}
