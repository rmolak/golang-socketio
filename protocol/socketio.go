package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	MessageOpen        = "0"
	MessageClose       = "1"
	MessagePing        = "2"
	MessagePingProbe   = "2probe"
	MessagePongProbe   = "3probe"
	MessagePong        = "3"
	messageMSG         = "4"
	MessageEmpty       = "40"
	messageCloseClient = "41"
	messageCommon      = "42"
	messageACK         = "43"
	MessageUpgrade     = "5"
	MessageBlank       = "6"
	MessageStub        = "stub"
)

var (
	ErrorWrongMessageType = errors.New("wrong message type")
	ErrorWrongPacket      = errors.New("wrong packet")
)

func typeToText(mType int) (string, error) {
	codesToNames := map[int]string{
		MessageTypeOpen:        MessageOpen,
		MessageTypeClose:       MessageClose,
		MessageTypePing:        MessagePing,
		MessageTypePong:        MessagePong,
		MessageTypeEmpty:       MessageEmpty,
		MessageTypeEmit:        messageCommon,
		MessageTypeAckRequest:  messageCommon,
		MessageTypeAckResponse: messageACK,
	}
	mName, exists := codesToNames[mType]
	if !exists {
		return "", ErrorWrongMessageType
	}
	return mName, nil
}

// Encode a socket.io message m to the protocol format
func Encode(m *Message) (string, error) {
	result, err := typeToText(m.Type)
	if err != nil {
		return "", err
	}

	switch m.Type {
	case MessageTypeEmpty, MessageTypePing, MessageTypePong:
		return result, nil
	case MessageTypeAckRequest:
		result += strconv.Itoa(m.AckID)
	case MessageTypeAckResponse:
		result += strconv.Itoa(m.AckID)
		return result + "[" + m.Args + "]", nil
	case MessageTypeOpen, MessageTypeClose:
		return result + m.Args, nil
	}

	jsonMethod, err := json.Marshal(&m.EventName)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`%s[%s,%s]`, result, string(jsonMethod), m.Args), nil
}

// MustEncode the message m acts like Encode but panics on error
func MustEncode(m *Message) string {
	result, err := Encode(m)
	if err != nil {
		panic(err)
	}
	return result
}

func getMessageType(data string) (int, error) {
	if len(data) == 0 {
		return 0, ErrorWrongMessageType
	}
	switch data[0:1] {
	case MessageOpen:
		return MessageTypeOpen, nil
	case MessageClose:
		return MessageTypeClose, nil
	case MessagePing:
		return MessageTypePing, nil
	case MessagePong:
		return MessageTypePong, nil
	case MessageUpgrade:
		return MessageTypeUpgrade, nil
	case MessageBlank:
		return MessageTypeBlank, nil
	case messageMSG:
		if len(data) == 1 {
			return 0, ErrorWrongMessageType
		}
		switch data[0:2] {
		case MessageEmpty:
			return MessageTypeEmpty, nil
		case messageCloseClient:
			return MessageTypeClose, nil
		case messageCommon:
			return MessageTypeAckRequest, nil
		case messageACK:
			return MessageTypeAckResponse, nil
		}
	}
	return 0, ErrorWrongMessageType
}

// getAck extracts an id of the current packet if present
func getAck(text string) (ackId int, restText string, err error) {
	if len(text) < 4 {
		return 0, "", ErrorWrongPacket
	}
	text = text[2:]

	pos := strings.IndexByte(text, '[')
	if pos == -1 {
		return 0, "", ErrorWrongPacket
	}

	ack, err := strconv.Atoi(text[0:pos])
	if err != nil {
		return 0, "", err
	}

	return ack, text[pos:], nil
}

// getMethod extracts a message event name of the current packet if present
func getMethod(text string) (event, restText string, err error) {
	var start, end, rest, countQuote int

	for i, c := range text {
		if c == '"' {
			switch countQuote {
			case 0:
				start = i + 1
			case 1:
				end, rest = i, i+1
			default:
				return "", "", ErrorWrongPacket
			}
			countQuote++
		}
		if c == ',' {
			if countQuote < 2 {
				continue
			}
			rest = i + 1
			break
		}
	}

	if (end < start) || (rest >= len(text)) {
		return "", "", ErrorWrongPacket
	}

	return text[start:end], text[rest : len(text)-1], nil
}

// Decode the given data string into a Message
func Decode(data string) (*Message, error) {
	var err error
	m := &Message{Source: data}

	m.Type, err = getMessageType(data)
	if err != nil {
		return nil, err
	}

	switch m.Type {
	case MessageTypeUpgrade, MessageTypeClose, MessageTypePing, MessageTypePong, MessageTypeEmpty, MessageTypeBlank:
		return m, nil
	case MessageTypeOpen:
		m.Args = data[1:]
		return m, nil
	}

	ack, rest, err := getAck(data)
	m.AckID = ack
	switch m.Type{
	case MessageTypeAckResponse, MessageTypeAckRequest:
		if err != nil {
			return nil, err
		}
		m.Args = rest[1 : len(rest)-1]
		return m, nil
	}

	if err != nil {
		m.Type = MessageTypeEmit
		rest = data[2:]
	}

	m.EventName, m.Args, err = getMethod(rest)
	if err != nil {
		return nil, err
	}

	return m, nil
}
