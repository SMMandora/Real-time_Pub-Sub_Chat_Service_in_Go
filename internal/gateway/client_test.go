package gateway

import (
	"testing"
)

// fakeRegistry records hub calls so handleFrame can be tested in isolation.
type fakeRegistry struct {
	joined    []string
	left      []string
	broadcast []Frame
}

func (f *fakeRegistry) Join(roomID string, m member)  { f.joined = append(f.joined, roomID) }
func (f *fakeRegistry) Leave(roomID string, m member) { f.left = append(f.left, roomID) }
func (f *fakeRegistry) Broadcast(roomID string, fr Frame) {
	f.broadcast = append(f.broadcast, fr)
}

func drain(c *Client) []Frame {
	var out []Frame
	for {
		select {
		case f := <-c.send:
			out = append(out, f)
		default:
			return out
		}
	}
}

func TestNewIDFormat(t *testing.T) {
	id := newID()
	if len(id) != 8 {
		t.Fatalf("expected 8 hex chars, got %q", id)
	}
}

func TestHandleSendRequiresJoin(t *testing.T) {
	reg := &fakeRegistry{}
	c := newClient(reg, func() {})

	c.handleFrame(Frame{Type: TypeSend, Room: "general", Text: "hi"})

	if len(reg.broadcast) != 0 {
		t.Fatalf("send before join should not broadcast, got %+v", reg.broadcast)
	}
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected one error frame, got %+v", out)
	}
}

func TestHandleJoinThenSend(t *testing.T) {
	reg := &fakeRegistry{}
	c := newClient(reg, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "general"})
	c.handleFrame(Frame{Type: TypeSend, Room: "general", Text: "hi"})

	if len(reg.joined) != 1 || reg.joined[0] != "general" {
		t.Fatalf("expected join general, got %+v", reg.joined)
	}
	if len(reg.broadcast) != 1 {
		t.Fatalf("expected one broadcast, got %+v", reg.broadcast)
	}
	got := reg.broadcast[0]
	if got.Type != TypeMessage || got.From != c.ID() || got.Text != "hi" || got.TS == 0 {
		t.Fatalf("unexpected broadcast frame: %+v", got)
	}
}

func TestHandleUnknownType(t *testing.T) {
	reg := &fakeRegistry{}
	c := newClient(reg, func() {})
	c.handleFrame(Frame{Type: "bogus"})
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected error frame for unknown type, got %+v", out)
	}
}

func TestEnqueueOverflowClosesClient(t *testing.T) {
	closed := false
	reg := &fakeRegistry{}
	c := newClient(reg, func() { closed = true })

	// Fill the buffer (cap 16) without draining, then overflow.
	for i := 0; i < cap(c.send)+1; i++ {
		c.enqueue(Frame{Type: TypeMessage})
	}
	if !closed {
		t.Fatal("expected overflow to trigger close/cancel")
	}
}

func TestLeaveAllLeavesEveryJoinedRoom(t *testing.T) {
	reg := &fakeRegistry{}
	c := newClient(reg, func() {})
	c.handleFrame(Frame{Type: TypeJoin, Room: "a"})
	c.handleFrame(Frame{Type: TypeJoin, Room: "b"})

	c.leaveAll()

	if len(reg.left) != 2 {
		t.Fatalf("expected to leave 2 rooms, got %+v", reg.left)
	}
}
