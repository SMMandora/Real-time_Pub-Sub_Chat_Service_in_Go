package gateway

import (
	"context"
	"sync"
)

type fakeRoomStore struct {
	mu        sync.Mutex
	rooms     map[string]RoomRecord
	lookupErr error
}

func newFakeRoomStore() *fakeRoomStore {
	return &fakeRoomStore{rooms: make(map[string]RoomRecord)}
}

func (s *fakeRoomStore) put(r RoomRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rooms[r.ID] = r
}

func (s *fakeRoomStore) Lookup(_ context.Context, id string) (RoomRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lookupErr != nil {
		return RoomRecord{}, false, s.lookupErr
	}
	r, ok := s.rooms[id]
	return r, ok, nil
}

func (s *fakeRoomStore) Create(_ context.Context, r RoomRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rooms[r.ID]; ok {
		return ErrRoomExists
	}
	s.rooms[r.ID] = r
	return nil
}

func (s *fakeRoomStore) List(_ context.Context) ([]RoomRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RoomRecord, 0, len(s.rooms))
	for _, r := range s.rooms {
		out = append(out, r)
	}
	return out, nil
}

func (s *fakeRoomStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rooms, id)
	return nil
}
