package gateway

import "encoding/json"

// Frame is the single JSON envelope for every message on the wire.
// omitempty keeps each frame minimal; unused fields are simply absent.
type Frame struct {
	Type    string `json:"type"`
	Room    string `json:"room,omitempty"`
	Text    string `json:"text,omitempty"`
	From    string `json:"from,omitempty"`
	TS      int64  `json:"ts,omitempty"`
	Event   string `json:"event,omitempty"`
	Message string `json:"message,omitempty"`
}

const (
	TypeJoin    = "join"
	TypeLeave   = "leave"
	TypeSend    = "send"
	TypeMessage = "message"
	TypeSystem  = "system"
	TypeError   = "error"
)

func messageFrame(room, from, text string, ts int64) Frame {
	return Frame{Type: TypeMessage, Room: room, From: from, Text: text, TS: ts}
}

func systemFrame(room, event, from string) Frame {
	return Frame{Type: TypeSystem, Room: room, Event: event, From: from}
}

func errorFrame(msg string) Frame {
	return Frame{Type: TypeError, Message: msg}
}

func (f Frame) encode() ([]byte, error) {
	return json.Marshal(f)
}

func decodeFrame(data []byte) (Frame, error) {
	var f Frame
	err := json.Unmarshal(data, &f)
	return f, err
}
