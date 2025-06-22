package stream

import (
	"fmt"
	"sync"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"
)

// Stream represents a Redis-like stream data structure.
type Stream struct {
	mu     sync.Mutex
	tree   art.Tree
	lastID StreamID
}

type StreamInsertRequest struct {
	streamId   string
	rawEntryId string
	entries    map[string]string
}

/** insert the enrties to the given stream id. if the stream doesn't exist generate a new stream and insert the entries
 */
// func ExecuteStreamEntryInsertion(insertionRequest StreamInsertRequest) (Stream, error) {

// }

// NewStream creates a new empty stream.
func NewStream() *Stream {
	return &Stream{
		tree:   art.New(),
		lastID: StreamID{Timestamp: 0, Sequence: 0},
	}
}

// AddEntry adds a new entry to the stream.
// If needAutoSeq is true, it will compute a new sequence number for the timestamp.
func (s *Stream) addEntry(baseID StreamID, needAutoSeq bool, fields map[string]string) (StreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var id StreamID

	if baseID.Timestamp == 0 && needAutoSeq {
		// Full auto ID generation
		now := currentTimeMillis()
		if now == s.lastID.Timestamp {
			s.lastID.Sequence++
		} else {
			s.lastID.Timestamp = now
			s.lastID.Sequence = 0
		}
		id = s.lastID
	} else {
		// Manual timestamp or full ID provided
		id.Timestamp = baseID.Timestamp
		if needAutoSeq {
			id.Sequence = s.nextSequenceForTimestamp(id.Timestamp)
		} else {
			id.Sequence = baseID.Sequence
		}

		// Ensure new ID is greater than last inserted
		if !s.lastID.LessThan(id) {
			return StreamID{}, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}

		s.lastID = id
	}

	// Store the entry
	entry := &StreamEntry{
		ID:     id,
		Fields: fields,
	}
	s.tree.Insert(art.Key(id.String()), entry)
	return id, nil
}

// nextSequenceForTimestamp finds the next available sequence for a given timestamp.
func (s *Stream) nextSequenceForTimestamp(timestamp int64) int64 {
	// Increment from the last sequence number used at this timestamp
	if s.lastID.Timestamp == timestamp {
		return s.lastID.Sequence + 1
	}
	return 0
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
