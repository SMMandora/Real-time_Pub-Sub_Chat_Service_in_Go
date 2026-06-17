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

	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/tracing"
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

// clientConfig groups the stable per-connection dependencies so newClient does
// not grow an unwieldy parameter list.
type clientConfig struct {
	hub      roomRegistry
	history  history
	presence PresenceStore
	limiter  RateLimiter
	rooms    RoomStore
	members  MemberStore
	log      *slog.Logger
}

// Client is one WebSocket connection. enqueue feeds the bounded send channel
// that writePump drains; overflow drops the client by cancelling its context.
type Client struct {
	id          string
	username    string
	ctx         context.Context
	hub         roomRegistry
	history     history
	presence    PresenceStore
	limiter     RateLimiter
	rooms       RoomStore
	members     MemberStore
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

func newClient(ctx context.Context, username string, cfg clientConfig, cancel context.CancelFunc) *Client {
	return &Client{
		id:         newID(),
		username:   username,
		ctx:        ctx,
		hub:        cfg.hub,
		history:    cfg.history,
		presence:   cfg.presence,
		limiter:    cfg.limiter,
		rooms:      cfg.rooms,
		members:    cfg.members,
		hbInterval: heartbeatInterval,
		log:        cfg.log,
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
		if !c.allowJoin(f.Room, f.Token) {
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
			c.touchMember(f.Room)
		}
	case TypeLeave:
		c.mu.Lock()
		was := c.joined[f.Room]
		delete(c.joined, f.Room)
		c.mu.Unlock()
		if was {
			c.hub.Leave(f.Room, c)
			c.removePresence(f.Room)
			c.touchMember(f.Room)
		}
	case TypeSend:
		c.mu.Lock()
		joined := c.joined[f.Room]
		c.mu.Unlock()
		if !joined {
			c.enqueue(errorFrame(fmt.Sprintf("not joined to room %q", f.Room)))
			return
		}
		if !c.allowSend() {
			c.enqueue(errorFrame("rate limit exceeded"))
			return
		}
		ctx, span := tracing.Tracer().Start(c.ctx, "chat.send")
		msg := messageFrame(f.Room, c.username, f.Text, nowMillis())
		msg.Trace = tracing.Inject(ctx)
		c.log.Info("message sent", "user", c.username, "room", f.Room,
			"trace_id", span.SpanContext().TraceID().String())
		err := c.hub.Publish(f.Room, msg)
		span.End()
		if err != nil {
			c.enqueue(errorFrame("failed to send message"))
		}
	case TypeTyping:
		c.mu.Lock()
		joined := c.joined[f.Room]
		c.mu.Unlock()
		if joined {
			_ = c.hub.PublishPresence(f.Room, typingFrame(f.Room, c.username))
		}
	default:
		c.enqueue(errorFrame("unknown frame type"))
	}
}

// allowJoin gates a join on room visibility. Unregistered or public rooms are
// allowed; a private room requires a matching token. A lookup error fails
// closed (rejects), so a DB blip cannot leak a private room. It enqueues the
// error frame on denial.
func (c *Client) allowJoin(room, token string) bool {
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
	defer cancel()
	rec, found, err := c.rooms.Lookup(ctx, room)
	if err != nil {
		c.log.Warn("room lookup failed", "room", room, "err", err)
		c.enqueue(errorFrame("room unavailable"))
		return false
	}
	if found && rec.Visibility == "private" && rec.InviteToken != token {
		c.enqueue(errorFrame("invalid invite token"))
		return false
	}
	return true
}

// allowSend consults the rate limiter. On a limiter error it fails open
// (allows) so a Redis blip does not block all chat.
func (c *Client) allowSend() bool {
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
	defer cancel()
	allowed, err := c.limiter.Allow(ctx, c.username)
	if err != nil {
		c.log.Warn("rate limit check failed", "user", c.username, "err", err)
		return true
	}
	return allowed
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

func (c *Client) touchMember(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.members.Touch(ctx, room, c.username, nowMillis()); err != nil {
		c.log.Warn("member touch failed", "room", room, "err", err)
	}
}

// addPresence records this client in the room's presence set and broadcasts the
// updated snapshot. Synchronous: join/leave are infrequent and the Redis calls
// are small. Uses a background context so it also works during teardown.
func (c *Client) addPresence(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.presence.Add(ctx, room, c.username, nowMillis()); err != nil {
		c.log.Warn("presence add failed", "room", room, "err", err)
		return
	}
	c.publishSnapshot(ctx, room)
}

func (c *Client) removePresence(room string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.presence.Remove(ctx, room, c.username)
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
				_ = c.presence.Add(ctx, room, c.username, nowMillis())
				_ = c.members.Touch(ctx, room, c.username, nowMillis())
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
		c.touchMember(room)
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
