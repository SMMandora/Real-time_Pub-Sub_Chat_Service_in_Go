package gateway

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// TestCrossGatewayFanout is the headline test: a publish on one gateway's hub
// reaches a member connected to a different gateway's hub, via Redis.
func TestCrossGatewayFanout(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	busA := NewRedisBus(mr.Addr())
	defer busA.Close()
	busB := NewRedisBus(mr.Addr())
	defer busB.Close()

	hubA := NewHub(busA)
	hubB := NewHub(busB)

	b := &fakeMember{id: "b"}
	hubB.Join("x", b) // gateway B subscribes room:x
	a := &fakeMember{id: "a"}
	hubA.Join("x", a) // gateway A subscribes room:x

	if err := hubA.Publish("x", messageFrame("x", "a", "hello", 1)); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return hasText(b.frames(), "hello") })
}
