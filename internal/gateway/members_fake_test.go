package gateway

import (
	"context"
	"sync"
)

type fakeMemberStore struct {
	mu      sync.Mutex
	members map[string][]MemberRecord
	touches []string
}

func newFakeMemberStore() *fakeMemberStore {
	return &fakeMemberStore{members: make(map[string][]MemberRecord)}
}

func (s *fakeMemberStore) put(room string, m MemberRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.members[room] = append(s.members[room], m)
}

func (s *fakeMemberStore) Touch(_ context.Context, room, username string, _ int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touches = append(s.touches, room+"/"+username)
	return nil
}

func (s *fakeMemberStore) List(_ context.Context, room string) ([]MemberRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.members[room], nil
}

func (s *fakeMemberStore) touchedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.touches)
}
