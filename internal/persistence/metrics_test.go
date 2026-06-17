package persistence

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestFlushUpdatesPersistenceMetrics(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1, time.Hour, testLogger())
	go b.Run()
	defer b.Close()

	before := testutil.ToFloat64(MessagesPersisted)
	b.Submit(Message{RoomID: "x", ID: 1, Body: "hi"})

	waitUntil(t, time.Second, func() bool {
		return testutil.ToFloat64(MessagesPersisted) >= before+1
	})
}
