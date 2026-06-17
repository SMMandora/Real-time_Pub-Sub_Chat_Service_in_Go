package gateway

import (
	"sync"
	"testing"
	"time"
)

// fakeMember is a test double for a room participant.
type fakeMember struct {
	id       string
	mu       sync.Mutex
	received []Frame
	closed   string
}

func (f *fakeMember) ID() string { return f.id }

func (f *fakeMember) enqueue(fr Frame) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = append(f.received, fr)
}

func (f *fakeMember) close(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = reason
}

func (f *fakeMember) frames() []Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Frame, len(f.received))
	copy(out, f.received)
	return out
}

func TestRoomFanout(t *testing.T) {
	r := newRoom("general")
	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}
	r.members["a"] = a
	r.members["b"] = b

	r.fanout(messageFrame("general", "a", "hi", 1))

	for _, m := range []*fakeMember{a, b} {
		got := m.frames()
		if len(got) != 1 || got[0].Text != "hi" {
			t.Fatalf("member %s got %+v", m.id, got)
		}
	}
}

func TestRoomRunJoinLeave(t *testing.T) {
	r := newRoom("general")
	go r.run()
	defer close(r.done)

	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}

	r.join <- a
	r.join <- b

	// Broadcast and let it propagate.
	r.broadcast <- messageFrame("general", "a", "hi", 1)

	waitFor(t, func() bool { return hasText(b.frames(), "hi") })

	// b leaves; room is not empty (a remains).
	reply := make(chan bool, 1)
	r.leave <- leaveReq{m: b, empty: reply}
	if empty := <-reply; empty {
		t.Fatal("room should not be empty while a remains")
	}

	// a leaves; room is now empty.
	r.leave <- leaveReq{m: a, empty: reply}
	if empty := <-reply; !empty {
		t.Fatal("room should be empty after last member leaves")
	}
}

func hasText(frames []Frame, text string) bool {
	for _, f := range frames {
		if f.Text == text {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}
