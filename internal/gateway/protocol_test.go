package gateway

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestFrameConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  Frame
		want Frame
	}{
		{
			name: "message",
			got:  messageFrame("general", "ab12", "hi", 1718600000000),
			want: Frame{Type: TypeMessage, Room: "general", From: "ab12", Text: "hi", TS: 1718600000000},
		},
		{
			name: "system",
			got:  systemFrame("general", "join", "ab12"),
			want: Frame{Type: TypeSystem, Room: "general", Event: "join", From: "ab12"},
		},
		{
			name: "error",
			got:  errorFrame("nope"),
			want: Frame{Type: TypeError, Message: "nope"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.got, tt.want) {
				t.Fatalf("got %+v want %+v", tt.got, tt.want)
			}
		})
	}
}

func TestFrameRoundTrip(t *testing.T) {
	in := messageFrame("general", "ab12", "hi", 1718600000000)
	data, err := in.encode()
	if err != nil {
		t.Fatal(err)
	}
	out, err := decodeFrame(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch: %+v != %+v", in, out)
	}
}

func TestEncodeOmitsEmpty(t *testing.T) {
	data, err := errorFrame("boom").encode()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["room"]; ok {
		t.Fatalf("expected room to be omitted, got %v", m)
	}
	if m["type"] != "error" || m["message"] != "boom" {
		t.Fatalf("unexpected payload: %v", m)
	}
}
