package main

import (
	"sync"
	"time"
)

type Store interface {
	Set(key string, value Entry)
	Get(key string) (Entry, bool)
	Keys() []string
	Delete(key string) bool
}

var store Store

func init() {
	store = NewInMemoryStore()
}

func NewInMemoryStore() Store {
	return &inMemoryStore{
		data: make(map[string]Entry),
	}
}

type inMemoryStore struct {
	mutex sync.RWMutex
	data  map[string]Entry
}

func (store *inMemoryStore) Set(key string, value Entry) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if !value.IsExpired() {
		store.data[key] = value
	}
}

func (store *inMemoryStore) Get(key string) (Entry, bool) {
	store.mutex.RLock()
	entry, ok := store.data[key]
	store.mutex.RUnlock()

	if !ok {
		return Entry{}, false
	}

	if entry.IsExpired() {
		store.Delete(key)
		return Entry{}, false
	}

	return entry, true
}

func (e Entry) IsExpired() bool {
	return e.ExpireAt != nil && time.Now().UnixMilli() >= *e.ExpireAt
}

func (store *inMemoryStore) Delete(key string) bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if _, exists := store.data[key]; exists {
		delete(store.data, key)
		return true
	}
	return false
}

func (store *inMemoryStore) Keys() []string {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	var keys []string

	for key, entry := range store.data {
		if entry.IsExpired() {
			store.Delete(key)
			continue
		}
		keys = append(keys, key)
	}

	return keys
}

type Entry struct {
	Val      string
	ExpireAt *int64
}
