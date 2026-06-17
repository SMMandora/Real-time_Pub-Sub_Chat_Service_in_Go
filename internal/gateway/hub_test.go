package gateway

import "testing"

func TestHubLazyCreateAndReap(t *testing.T) {
	h := NewHub()
	a := &fakeMember{id: "a"}

	h.Join("general", a)
	if h.roomCount() != 1 {
		t.Fatalf("expected 1 room, got %d", h.roomCount())
	}

	h.Leave("general", a)
	if h.roomCount() != 0 {
		t.Fatalf("expected room reaped, got %d", h.roomCount())
	}
}

func TestHubBroadcastReachesMembers(t *testing.T) {
	h := NewHub()
	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}
	h.Join("general", a)
	h.Join("general", b)

	h.Broadcast("general", messageFrame("general", "a", "hi", 1))

	waitFor(t, func() bool { return hasText(a.frames(), "hi") && hasText(b.frames(), "hi") })
}

func TestHubLeaveUnknownRoomIsNoop(t *testing.T) {
	h := NewHub()
	a := &fakeMember{id: "a"}
	h.Leave("ghost", a) // must not panic
	if h.roomCount() != 0 {
		t.Fatalf("expected 0 rooms, got %d", h.roomCount())
	}
}

func TestHubCloseAllClosesRegisteredClients(t *testing.T) {
	h := NewHub()
	a := &fakeMember{id: "a"}
	b := &fakeMember{id: "b"}
	h.Register(a)
	h.Register(b)

	h.CloseAll("bye")

	if a.closed != "bye" || b.closed != "bye" {
		t.Fatalf("expected both closed with reason, got a=%q b=%q", a.closed, b.closed)
	}
}
