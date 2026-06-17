package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

// fakeRegistry records hub calls so handleFrame can be tested in isolation.
type fakeRegistry struct {
	joined     []string
	left       []string
	published  []Frame
	publishErr error
}

func (f *fakeRegistry) Join(roomID string, m member)  { f.joined = append(f.joined, roomID) }
func (f *fakeRegistry) Leave(roomID string, m member) { f.left = append(f.left, roomID) }
func (f *fakeRegistry) Publish(roomID string, fr Frame) error {
	f.published = append(f.published, fr)
	return f.publishErr
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

func newTestClient(reg roomRegistry, hist history, cancel context.CancelFunc) *Client {
	return newClient(context.Background(), reg, hist, slog.New(slog.NewTextHandler(io.Discard, nil)), cancel)
}

func waitForFrames(t *testing.T, c *Client, n int) []Frame {
	t.Helper()
	var out []Frame
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		out = append(out, drain(c)...)
		if len(out) >= n {
			return out
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected %d frames, got %d", n, len(out))
	return nil
}

func TestNewIDFormat(t *testing.T) {
	id := newID()
	if len(id) != 8 {
		t.Fatalf("expected 8 hex chars, got %q", id)
	}
}

func TestHandleSendRequiresJoin(t *testing.T) {
	reg := &fakeRegistry{}
	c := newTestClient(reg, &fakeHistory{}, func() {})

	c.handleFrame(Frame{Type: TypeSend, Room: "general", Text: "hi"})

	if len(reg.published) != 0 {
		t.Fatalf("send before join should not broadcast, got %+v", reg.published)
	}
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected one error frame, got %+v", out)
	}
}

func TestHandleJoinThenSend(t *testing.T) {
	reg := &fakeRegistry{}
	c := newTestClient(reg, &fakeHistory{}, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "general"})
	c.handleFrame(Frame{Type: TypeSend, Room: "general", Text: "hi"})

	if len(reg.joined) != 1 || reg.joined[0] != "general" {
		t.Fatalf("expected join general, got %+v", reg.joined)
	}
	if len(reg.published) != 1 {
		t.Fatalf("expected one broadcast, got %+v", reg.published)
	}
	got := reg.published[0]
	if got.Type != TypeMessage || got.From != c.ID() || got.Text != "hi" || got.TS == 0 {
		t.Fatalf("unexpected broadcast frame: %+v", got)
	}
}

func TestHandleUnknownType(t *testing.T) {
	reg := &fakeRegistry{}
	c := newTestClient(reg, &fakeHistory{}, func() {})
	c.handleFrame(Frame{Type: "bogus"})
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected error frame for unknown type, got %+v", out)
	}
}

func TestEnqueueOverflowClosesClient(t *testing.T) {
	closed := false
	reg := &fakeRegistry{}
	c := newTestClient(reg, &fakeHistory{}, func() { closed = true })

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
	c := newTestClient(reg, &fakeHistory{}, func() {})
	c.handleFrame(Frame{Type: TypeJoin, Room: "a"})
	c.handleFrame(Frame{Type: TypeJoin, Room: "b"})

	c.leaveAll()

	if len(reg.left) != 2 {
		t.Fatalf("expected to leave 2 rooms, got %+v", reg.left)
	}
}

func TestHandleLeaveUnjoinedIsNoop(t *testing.T) {
	reg := &fakeRegistry{}
	c := newTestClient(reg, &fakeHistory{}, func() {})
	c.handleFrame(Frame{Type: TypeLeave, Room: "ghost"})
	if len(reg.left) != 0 {
		t.Fatalf("leave of unjoined room should not call hub.Leave, got %+v", reg.left)
	}
}

func TestHandleSendPublishErrorReturnsErrorFrame(t *testing.T) {
	reg := &fakeRegistry{publishErr: errors.New("boom")}
	c := newTestClient(reg, &fakeHistory{}, func() {})
	c.handleFrame(Frame{Type: TypeJoin, Room: "general"})
	c.handleFrame(Frame{Type: TypeSend, Room: "general", Text: "hi"})
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected error frame on publish failure, got %+v", out)
	}
}

func TestJoinReplaysRecentWithoutCursor(t *testing.T) {
	reg := &fakeRegistry{}
	hist := &fakeHistory{recent: []StoredMessage{
		{ID: 1, From: "u", Text: "a", TS: 1},
		{ID: 2, From: "u", Text: "b", TS: 2},
	}}
	c := newTestClient(reg, hist, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})

	got := waitForFrames(t, c, 2)
	if got[0].Type != TypeMessage || got[0].ID != 1 || got[1].ID != 2 {
		t.Fatalf("unexpected replay frames: %+v", got)
	}
}

func TestJoinReplaysSinceWithCursor(t *testing.T) {
	reg := &fakeRegistry{}
	hist := &fakeHistory{since: []StoredMessage{{ID: 43, From: "u", Text: "c", TS: 3}}}
	c := newTestClient(reg, hist, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x", ID: 42})

	got := waitForFrames(t, c, 1)
	if got[0].ID != 43 {
		t.Fatalf("expected replayed id 43, got %+v", got)
	}
	if hist.sinceCalledWith() != 42 {
		t.Fatalf("expected Since called with cursor 42, got %d", hist.sinceCalledWith())
	}
}

func TestJoinReplayErrorEnqueuesNothing(t *testing.T) {
	reg := &fakeRegistry{}
	hist := &fakeHistory{err: errors.New("db down")}
	c := newTestClient(reg, hist, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})

	time.Sleep(100 * time.Millisecond)
	if got := drain(c); len(got) != 0 {
		t.Fatalf("expected no frames on history error, got %+v", got)
	}
}
