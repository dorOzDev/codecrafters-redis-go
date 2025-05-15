package main

import "sync"

type Store interface {
	Set(key, value string)
	Get(key string) (string, bool)
}

var store Store

func init() {
	store = NewInMemoryStore()
}

func NewInMemoryStore() Store {
	return &inMemoryStore{
		data: make(map[string]string),
	}
}

type inMemoryStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func (s *inMemoryStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *inMemoryStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}
