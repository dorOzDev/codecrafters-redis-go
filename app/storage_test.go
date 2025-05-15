package main

func ResetStore() {
	if s, ok := store.(*inMemoryStore); ok {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		s.data = make(map[string]Entry)
	}
}
