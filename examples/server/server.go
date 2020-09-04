package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/rmolak/golang-socketio"
	"github.com/rmolak/golang-socketio/examples/model"
)

var assetsDir http.FileSystem

const port = 3811

const OK = "OK"

const (
	eventJoin  = "join"
	eventSend  = "send"
	eventLeave = "leave"
)

func onConnectionHandler(c *gosocketio.Channel)    { log.Printf("Connected %s\n", c.Id()) }
func onDisconnectionHandler(c *gosocketio.Channel) { log.Printf("Disconnected %s\n", c.Id()) }
func onJoinHandler(c *gosocketio.Channel, roomName string) string {
	log.Printf("Join %s to room %s\n", c.Id(), roomName)
	if err := c.Join(roomName); err != nil {
		return err.Error()
	}
	return OK
}
func onSendHandler(c *gosocketio.Channel, param interface{}) interface{} {
	log.Printf("Received SEND on %s with ", c.Id())
	j, err := json.Marshal(param)
	if err != nil {
		log.Println("error:", err)
		return err
	}
	log.Printf("payload JSON is %s\n", j)

	var obj model.Data
	if err := json.Unmarshal(j, &obj); err != nil {
		log.Println("error:", err)
		return err
	}

	if obj.BroadcastRoomName == "" {
		if err := c.Emit(obj.EventName, obj.Payload); err != nil {
			return err
		}
		return OK
	}

	c.BroadcastTo(obj.BroadcastRoomName, obj.EventName, obj.Payload)
	return OK
}
func onLeaveHandler(c *gosocketio.Channel, roomName string) string {
	log.Printf("Leave %s from room %s\n", c.Id(), roomName)
	if err := c.Leave(roomName); err != nil {
		return err.Error()
	}
	return OK
}

func main() {
	currentRoot, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	d := filepath.Join(currentRoot, "..", "assets")
	if !fileExists(d) {
		d = filepath.Join(currentRoot, "examples", "assets")
	}

	assetsDir = http.Dir(d)

	log.Println("assetsDir:", assetsDir)

	server := gosocketio.NewServer()
	if err := server.On(gosocketio.OnConnection, onConnectionHandler); err != nil {
		log.Fatal(err)
	}
	if err := server.On(gosocketio.OnDisconnection, onDisconnectionHandler); err != nil {
		log.Fatal(err)
	}
	if err := server.On(eventJoin, onJoinHandler); err != nil {
		log.Fatal(err)
	}
	if err := server.On(eventSend, onSendHandler); err != nil {
		log.Fatal(err)
	}
	if err := server.On(eventLeave, onLeaveHandler); err != nil {
		log.Fatal(err)
	}

	serveMux := http.NewServeMux()
	serveMux.Handle("/socket.io/", server)
	serveMux.HandleFunc("/", assetsFileHandler)

	log.Println("Starting server...")
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), serveMux); err != nil {
		log.Panic(err)
	}
}

// fileExists returns true if a file with given path exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// assetsFileHandler for static Data
func assetsFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return
	}

	file := r.URL.Path
	f, err := assetsDir.Open(file)
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	http.ServeContent(w, r, file, fi.ModTime(), f)
}
