package store

import (
	"sync"
)

// Store is a thread-safe in-memory key-value store
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewStore creates a new in-memory store
func NewStore() *Store {
	return &Store{
		data: make(map[string][]byte),
	}
}

// Put stores a key-value pair
func (s *Store) Put(key string, val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = val
}

// Get retrieves a value by key
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Delete removes a key-value pair
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}

// Len returns the number of keys in the store
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Dump returns a deep copy of the current store for snapshotting
func (s *Store) Dump() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copyMap := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		vv := make([]byte, len(v))
		copy(vv, v)
		copyMap[k] = vv
	}
	return copyMap
}

// Load replaces the store content with the provided state
func (s *Store) Load(state map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string][]byte, len(state))
	for k, v := range state {
		vv := make([]byte, len(v))
		copy(vv, v)
		s.data[k] = vv
	}
}

