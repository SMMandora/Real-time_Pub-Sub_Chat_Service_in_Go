package gateway

import (
	"encoding/json"
	"testing"
)

func TestHubLazyCreateAndReapAndSubscribe(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	a := &fakeMember{id: "a"}

	h.Join("general", a)
	if h.roomCount() != 1 {
		t.Fatalf("expected 1 room, got %d", h.roomCount())
	}
	if !bus.isSubscribed(roomChannel("general")) {
		t.Fatalf("expected subscription to %q after join", roomChannel("general"))
	}

	h.Leave("general", a)
	if h.roomCount() != 0 {
		t.Fatalf("expected room reaped, got %d", h.roomCount())
	}
	if bus.isSubscribed(roomChannel("general")) {
		t.Fatalf("expected unsubscribe from %q after reap", roomChannel("general"))
	}
}

func TestHubSubscribesOncePerRoom(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	h.Join("general", &fakeMember{id: "a"})
	h.Join("general", &fakeMember{id: "b"})
	if bus.subscribeCount() != 1 {
		t.Fatalf("expected 1 subscribe for two joiners of same room, got %d", bus.subscribeCount())
	}
}

func TestHubPublishGoesToBus(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	h.Join("general", &fakeMember{id: "a"})

	if err := h.Publish("general", messageFrame("general", "a", "hi", 1)); err != nil {
		t.Fatal(err)
	}
	if bus.publishCount() != 1 {
		t.Fatalf("expected 1 publish, got %d", bus.publishCount())
	}
}

func TestHubRoundTripReachesMembers(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}
	h.Join("general", a)
	h.Join("general", b)

	// Publish loops back through the subscribed bus to local members.
	if err := h.Publish("general", messageFrame("general", "a", "hi", 1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return hasText(a.frames(), "hi") && hasText(b.frames(), "hi") })
}

func TestHubLeaveUnknownRoomIsNoop(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	a := &fakeMember{id: "a"}
	h.Leave("ghost", a)
	if h.roomCount() != 0 {
		t.Fatalf("expected 0 rooms, got %d", h.roomCount())
	}
}

func TestHubCloseAllClosesRegisteredClients(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}
	h.Register(a)
	h.Register(b)

	h.CloseAll("bye")

	if a.closed != "bye" || b.closed != "bye" {
		t.Fatalf("expected both closed with reason, got a=%q b=%q", a.closed, b.closed)
	}
}

func TestHubPublishStampsIncreasingID(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	h.Join("general", &fakeMember{id: "a"})

	_ = h.Publish("general", messageFrame("general", "a", "one", 1))
	_ = h.Publish("general", messageFrame("general", "a", "two", 1))

	payloads := bus.publishedFrames()
	if len(payloads) != 2 {
		t.Fatalf("expected 2 published frames, got %d", len(payloads))
	}
	var f1, f2 Frame
	_ = json.Unmarshal(payloads[0], &f1)
	_ = json.Unmarshal(payloads[1], &f2)
	if f1.ID != 1 || f2.ID != 2 {
		t.Fatalf("expected stamped ids 1,2, got %d,%d", f1.ID, f2.ID)
	}
}
