package gateway

// member is anything that can participate in a room. *Client implements it;
// tests use a fake. Methods are unexported because only this package drives them.
type member interface {
	ID() string
	enqueue(Frame)
	close(reason string)
}

// leaveReq carries a leave plus a reply channel reporting whether the room is
// now empty, so the hub can decide to reap the room.
type leaveReq struct {
	m     member
	empty chan bool
}

// Room owns its member set inside a single goroutine (run). All mutation flows
// through channels, so there are no locks on the fan-out path.
type Room struct {
	id        string
	join      chan member
	leave     chan leaveReq
	broadcast chan Frame
	done      chan struct{}
	members   map[string]member
}

func newRoom(id string) *Room {
	return &Room{
		id:        id,
		join:      make(chan member),
		leave:     make(chan leaveReq),
		broadcast: make(chan Frame),
		done:      make(chan struct{}),
		members:   make(map[string]member),
	}
}

func (r *Room) run() {
	for {
		select {
		case m := <-r.join:
			r.members[m.ID()] = m
			r.fanout(systemFrame(r.id, "join", m.ID()))
		case req := <-r.leave:
			if _, ok := r.members[req.m.ID()]; ok {
				delete(r.members, req.m.ID())
				r.fanout(systemFrame(r.id, "leave", req.m.ID()))
			}
			req.empty <- (len(r.members) == 0)
		case f := <-r.broadcast:
			r.fanout(f)
		case <-r.done:
			return
		}
	}
}

func (r *Room) fanout(f Frame) {
	for _, m := range r.members {
		m.enqueue(f)
	}
}
