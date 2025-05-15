package main

func ResetStore() {
	if s, ok := store.(*inMemoryStore); ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.data = make(map[string]string)
	}
}
