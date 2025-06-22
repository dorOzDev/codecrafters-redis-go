package stream

import (
	"fmt"
	"strconv"
	"strings"
)

type StreamID struct {
	Timestamp int64
	Sequence  int64
}

func (id StreamID) String() string {
	return fmt.Sprintf("%d-%d", id.Timestamp, id.Sequence)
}

func ParseStreamID(raw string) (StreamID, bool, error) {
	parts := strings.Split(raw, "-")

	switch len(parts) {
	case 1:
		timestamp, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return StreamID{}, false, fmt.Errorf("invalid timestamp in stream ID: %w", err)
		}
		return StreamID{Timestamp: timestamp, Sequence: 0}, true, nil

	case 2:
		timestamp, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return StreamID{}, false, fmt.Errorf("invalid timestamp in stream ID: %w", err)
		}
		sequence, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return StreamID{}, false, fmt.Errorf("invalid sequence in stream ID: %w", err)
		}
		return StreamID{Timestamp: timestamp, Sequence: sequence}, false, nil

	default:
		return StreamID{}, false, fmt.Errorf("invalid stream ID format: %s", raw)
	}
}

func (id StreamID) LessThan(other StreamID) bool {
	if id.Timestamp < other.Timestamp {
		return true
	}
	if id.Timestamp == other.Timestamp {
		return id.Sequence < other.Sequence
	}
	return false
}

// Equal returns true if both IDs are exactly the same
func (id StreamID) Equal(other StreamID) bool {
	return id.Timestamp == other.Timestamp && id.Sequence == other.Sequence
}
