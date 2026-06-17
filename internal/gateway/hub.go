package gateway

import "sync"

// Hub is a registry of rooms and connected clients. The mutex guards the maps
// only; it is NOT held during fan-out (that lives in each Room's goroutine).
type Hub struct {
	mu      sync.Mutex
	rooms   map[string]*Room
	clients map[string]member
}

func NewHub() *Hub {
	return &Hub{
		rooms:   make(map[string]*Room),
		clients: make(map[string]member),
	}
}

// Register/Unregister track every connected client so CloseAll can reach
// clients that are connected but not in any room.
func (h *Hub) Register(m member) {
	h.mu.Lock()
	h.clients[m.ID()] = m
	h.mu.Unlock()
}

func (h *Hub) Unregister(m member) {
	h.mu.Lock()
	delete(h.clients, m.ID())
	h.mu.Unlock()
}

// Join adds a member to a room, creating the room (and its goroutine) on first
// use. The lock is held across the channel send so Join and Leave fully
// serialize, preventing a reap from racing an in-flight join.
func (h *Hub) Join(roomID string, m member) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[roomID]
	if !ok {
		r = newRoom(roomID)
		h.rooms[roomID] = r
		go r.run()
	}
	r.join <- m
}

// Leave removes a member and reaps the room if it became empty. The reply from
// the room arrives while the lock is held, so the reap decision is atomic with
// respect to Join.
func (h *Hub) Leave(roomID string, m member) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.rooms[roomID]
	if !ok {
		return
	}
	reply := make(chan bool, 1)
	r.leave <- leaveReq{m: m, empty: reply}
	if <-reply {
		close(r.done)
		delete(h.rooms, roomID)
	}
}

// Broadcast delivers a frame to a room without holding the lock during the
// send. If the room was reaped concurrently, the select on r.done abandons the
// send instead of blocking forever.
func (h *Hub) Broadcast(roomID string, f Frame) {
	h.mu.Lock()
	r, ok := h.rooms[roomID]
	h.mu.Unlock()
	if !ok {
		return
	}
	select {
	case r.broadcast <- f:
	case <-r.done:
	}
}

func (h *Hub) CloseAll(reason string) {
	h.mu.Lock()
	members := make([]member, 0, len(h.clients))
	for _, m := range h.clients {
		members = append(members, m)
	}
	h.mu.Unlock()
	for _, m := range members {
		m.close(reason)
	}
}

// roomCount is a test helper for asserting lazy creation and reaping.
func (h *Hub) roomCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.rooms)
}
