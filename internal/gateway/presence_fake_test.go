package gateway

import (
	"context"
	"sort"
	"sync"
)

type fakePresenceStore struct {
	mu       sync.Mutex
	members  map[string]map[string]int64 // room -> member -> score
	addCalls int
	err      error
}

func newFakePresenceStore() *fakePresenceStore {
	return &fakePresenceStore{members: make(map[string]map[string]int64)}
}

func (s *fakePresenceStore) Add(_ context.Context, room, member string, score int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addCalls++
	if s.err != nil {
		return s.err
	}
	if s.members[room] == nil {
		s.members[room] = make(map[string]int64)
	}
	s.members[room][member] = score
	return nil
}

func (s *fakePresenceStore) Remove(_ context.Context, room, member string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.members[room] != nil {
		delete(s.members[room], member)
	}
	return nil
}

func (s *fakePresenceStore) Snapshot(_ context.Context, room string, minScore int64) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	var out []string
	for m, sc := range s.members[room] {
		if sc >= minScore {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *fakePresenceStore) addCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addCalls
}
