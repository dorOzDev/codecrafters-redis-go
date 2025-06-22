package stream

// StreamEntry represents a single entry in a Redis stream.
type StreamEntry struct {
	ID     StreamID
	Fields map[string]string
}
