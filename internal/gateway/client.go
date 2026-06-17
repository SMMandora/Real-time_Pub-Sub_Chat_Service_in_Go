package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

// replayLimit caps how many history messages are replayed to a joining client.
const replayLimit = 100

// roomRegistry is the subset of the hub a client depends on, so handleFrame can
// be tested with a fake.
type roomRegistry interface {
	Join(roomID string, m member)
	Leave(roomID string, m member)
	Publish(roomID string, f Frame) error
	PublishPresence(roomID string, f Frame) error
}

// Client is one WebSocket connection. enqueue feeds the bounded send channel
// that writePump drains; overflow drops the client by cancelling its context.
type Client struct {
	id          string
	ctx         context.Context
	hub         roomRegistry
	history     history
	presence    PresenceStore
	hbInterval  time.Duration
	log         *slog.Logger
	send        chan Frame
	cancel      context.CancelFunc
	once        sync.Once
	closeReason string

	mu     sync.Mutex
	joined map[string]bool
}

func newID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}

func newClient(ctx context.Context, hub roomRegistry, hist history, presence PresenceStore, log *slog.Logger, cancel context.CancelFunc) *Client {
	return &Client{
		id:         newID(),
		ctx:        ctx,
		hub:        hub,
		history:    hist,
		presence:   presence,
		hbInterval: heartbeatInterval,
		log:        log,
		send:       make(chan Frame, 16),
		cancel:     cancel,
		joined:     make(map[string]bool),
	}
}

func (c *Client) ID() string { return c.id }

func (c *Client) enqueue(f Frame) {
	select {
	case c.send <- f:
	default:
		c.close("slow consumer")
	}
}

func (c *Client) close(reason string) {
	c.once.Do(func() {
		c.closeReason = reason
		c.cancel()
	})
}

func (c *Client) handleFrame(f Frame) {
	switch f.Type {
	case TypeJoin:
		if f.Room == "" {
			c.enqueue(errorFrame("join requires a room"))
			return
		}
		c.mu.Lock()
		already := c.joined[f.Room]
		c.joined[f.Room] = true
		c.mu.Unlock()
		if !already {
			c.hub.Join(f.Room, c)
			// Replay history to this client only. Async so readPump is not
			// blocked on the DB; the client deduplicates by id, so replayed
			// and live messages may overlap harmlessly.
			go c.replay(f.Room, f.ID)
			c.addPresence(f.Room)
		}
	case TypeLeave:
		c.mu.Lock()
		was := c.joined[f.Room]
		delete(c.joined, f.Room)
		c.mu.Unlock()
		if was {
			c.hub.Leave(f.Room, c)
			c.removePresence(f.Room)
		}
	case TypeSend:
		c.mu.Lock()
		joined := c.joined[f.Room]
		c.mu.Unlock()
		if !joined {
			c.enqueue(errorFrame(fmt.Sprintf("not joined to room %q", f.Room)))
			return
		}
		if err := c.hub.Publish(f.Room, messageFrame(f.Room, c.id, f.Text, nowMillis())); err != nil {
			c.enqueue(errorFrame("failed to send message"))
		}
	case TypeTyping:
		c.mu.Lock()
		joined := c.joined[f.Room]
		c.mu.Unlock()
		if joined {
			_ = c.hub.PublishPresence(f.Room, typingFrame(f.Room, c.id))
		}
	default:
		c.enqueue(errorFrame("unknown frame type"))
	}
}

// replay fetches recent history (or messages after sinceID) and enqueues them
// to this client as message frames. Best-effort: on error it logs and returns.
func (c *Client) replay(room string, sinceID int64) {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	var msgs []StoredMessage
	var err error
	if sinceID > 0 {
		msgs, err = c.history.Since(ctx, room, sinceID, replayLimit)
	} else {
		msgs, err = c.history.Recent(ctx, room, replayLimit)
	}
	if err != nil {
		c.log.Warn("history replay failed", "room", room, "err", err)
		return
	}
	for _, m := range msgs {
		c.enqueue(Frame{Type: TypeMessage, Room: room, ID: m.ID, From: m.From, Text: m.Text, TS: m.TS})
	}
}

// addPresence records this client in the room's presence set and broadcasts the
// updated snapshot. Synchronous: join/leave are infrequent and the Redis calls
// are small. Uses a background context so it also works during teardown.
func (c *Client) addPresence(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.presence.Add(ctx, room, c.id, nowMillis()); err != nil {
		c.log.Warn("presence add failed", "room", room, "err", err)
		return
	}
	c.publishSnapshot(ctx, room)
}

func (c *Client) removePresence(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.presence.Remove(ctx, room, c.id)
	c.publishSnapshot(ctx, room)
}

func (c *Client) publishSnapshot(ctx context.Context, room string) {
	members, err := c.presence.Snapshot(ctx, room, nowMillis()-presenceTTLms)
	if err != nil {
		c.log.Warn("presence snapshot failed", "room", room, "err", err)
		return
	}
	_ = c.hub.PublishPresence(room, presenceFrame(room, members))
}

// heartbeat periodically refreshes this client's presence score in every joined
// room. It does not publish (membership is unchanged); it just keeps scores
// fresh so the client is not pruned by the TTL filter.
func (c *Client) heartbeat() {
	ticker := time.NewTicker(c.hbInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			rooms := make([]string, 0, len(c.joined))
			for room := range c.joined {
				rooms = append(rooms, room)
			}
			c.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			for _, room := range rooms {
				_ = c.presence.Add(ctx, room, c.id, nowMillis())
			}
			cancel()
		}
	}
}

func (c *Client) leaveAll() {
	c.mu.Lock()
	rooms := make([]string, 0, len(c.joined))
	for room := range c.joined {
		rooms = append(rooms, room)
	}
	c.joined = make(map[string]bool)
	c.mu.Unlock()
	for _, room := range rooms {
		c.hub.Leave(room, c)
		c.removePresence(room)
	}
}

func (c *Client) readPump(ctx context.Context, conn *websocket.Conn) {
	conn.SetReadLimit(65536)
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		f, err := decodeFrame(data)
		if err != nil {
			c.enqueue(errorFrame("invalid JSON"))
			continue
		}
		c.handleFrame(f)
	}
}

func (c *Client) writePump(ctx context.Context, conn *websocket.Conn) {
	for {
		select {
		case f := <-c.send:
			data, err := f.encode()
			if err != nil {
				continue
			}
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = conn.Write(wctx, websocket.MessageText, data)
			cancel()
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
