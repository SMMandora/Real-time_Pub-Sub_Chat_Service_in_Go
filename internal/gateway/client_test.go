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
	presence   []Frame
	publishErr error
}

func (f *fakeRegistry) Join(roomID string, m member)  { f.joined = append(f.joined, roomID) }
func (f *fakeRegistry) Leave(roomID string, m member) { f.left = append(f.left, roomID) }
func (f *fakeRegistry) Publish(roomID string, fr Frame) error {
	f.published = append(f.published, fr)
	return f.publishErr
}

func (f *fakeRegistry) PublishPresence(room string, fr Frame) error {
	f.presence = append(f.presence, fr)
	return nil
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
	cfg := clientConfig{
		hub:      reg,
		history:  hist,
		presence: newFakePresenceStore(),
		limiter:  &fakeRateLimiter{allow: true},
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return newClient(context.Background(), "tester", cfg, cancel)
}

func newPresenceClient(ctx context.Context, reg roomRegistry, ps PresenceStore, cancel context.CancelFunc) *Client {
	cfg := clientConfig{
		hub:      reg,
		history:  &fakeHistory{},
		presence: ps,
		limiter:  &fakeRateLimiter{allow: true},
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return newClient(ctx, "tester", cfg, cancel)
}

func newRateClient(reg roomRegistry, limiter RateLimiter) *Client {
	cfg := clientConfig{
		hub:      reg,
		history:  &fakeHistory{},
		presence: newFakePresenceStore(),
		limiter:  limiter,
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return newClient(context.Background(), "alice", cfg, func() {})
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
	if got.Type != TypeMessage || got.From != c.username || got.Text != "hi" || got.TS == 0 {
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

func TestJoinAddsPresenceAndBroadcastsSnapshot(t *testing.T) {
	reg := &fakeRegistry{}
	ps := newFakePresenceStore()
	c := newPresenceClient(context.Background(), reg, ps, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})

	if ps.addCallCount() == 0 {
		t.Fatal("expected presence Add on join")
	}
	if len(reg.presence) != 1 || reg.presence[0].Type != TypePresence || reg.presence[0].Room != "x" {
		t.Fatalf("expected one presence snapshot frame, got %+v", reg.presence)
	}
	if len(reg.presence[0].Members) != 1 || reg.presence[0].Members[0] != c.username {
		t.Fatalf("expected snapshot to contain joiner, got %+v", reg.presence[0].Members)
	}
}

func TestLeaveRemovesPresenceAndBroadcasts(t *testing.T) {
	reg := &fakeRegistry{}
	ps := newFakePresenceStore()
	c := newPresenceClient(context.Background(), reg, ps, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeLeave, Room: "x"})

	// After leave, the latest snapshot must not contain the member.
	last := reg.presence[len(reg.presence)-1]
	if len(last.Members) != 0 {
		t.Fatalf("expected empty snapshot after leave, got %+v", last.Members)
	}
}

func TestTypingPublishesTypingFrame(t *testing.T) {
	reg := &fakeRegistry{}
	c := newTestClient(reg, &fakeHistory{}, func() {})

	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeTyping, Room: "x"})

	var typing *Frame
	for i := range reg.presence {
		if reg.presence[i].Type == TypeTyping {
			typing = &reg.presence[i]
		}
	}
	if typing == nil || typing.Room != "x" || typing.From != c.username {
		t.Fatalf("expected a typing frame from %s in room x, got %+v", c.username, reg.presence)
	}
}

func TestTypingRequiresJoin(t *testing.T) {
	reg := &fakeRegistry{}
	c := newTestClient(reg, &fakeHistory{}, func() {})

	c.handleFrame(Frame{Type: TypeTyping, Room: "x"})

	for _, f := range reg.presence {
		if f.Type == TypeTyping {
			t.Fatal("typing before join should not publish")
		}
	}
}

func TestHeartbeatRefreshesPresence(t *testing.T) {
	reg := &fakeRegistry{}
	ps := newFakePresenceStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newPresenceClient(ctx, reg, ps, cancel)
	c.hbInterval = 10 * time.Millisecond

	c.mu.Lock()
	c.joined["x"] = true
	c.mu.Unlock()

	go c.heartbeat()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if ps.addCallCount() >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected heartbeat to call Add repeatedly, got %d", ps.addCallCount())
}

func TestSendBlockedByRateLimit(t *testing.T) {
	reg := &fakeRegistry{}
	c := newRateClient(reg, &fakeRateLimiter{allow: false})
	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeSend, Room: "x", Text: "hi"})

	if len(reg.published) != 0 {
		t.Fatalf("rate-limited send must not publish, got %+v", reg.published)
	}
	out := drain(c)
	if len(out) != 1 || out[0].Type != TypeError {
		t.Fatalf("expected one rate-limit error frame, got %+v", out)
	}
}

func TestSendAllowedByRateLimit(t *testing.T) {
	reg := &fakeRegistry{}
	c := newRateClient(reg, &fakeRateLimiter{allow: true})
	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeSend, Room: "x", Text: "hi"})

	if len(reg.published) != 1 || reg.published[0].From != "alice" {
		t.Fatalf("expected one published message from alice, got %+v", reg.published)
	}
}

func TestSendFailsOpenOnLimiterError(t *testing.T) {
	reg := &fakeRegistry{}
	c := newRateClient(reg, &fakeRateLimiter{allow: false, err: errors.New("redis down")})
	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeSend, Room: "x", Text: "hi"})

	if len(reg.published) != 1 {
		t.Fatalf("a limiter error should fail open and publish, got %+v", reg.published)
	}
}
