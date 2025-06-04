package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

type RESPValueGenerator interface {
	Parse(reader *TrackingBufReader) (RESPValue, error)
}

var respParsers = map[RESPType]RESPValueGenerator{
	SimpleString: simpleStringParser{},
	Error:        errorParser{},
	Integer:      integerParser{},
	BulkString:   bulkStringParser{},
	Array:        arrayParser{},
}

type simpleStringParser struct{}

func (p simpleStringParser) Parse(reader *TrackingBufReader) (RESPValue, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return RESPValue{}, err
	}
	return RESPValue{Type: SimpleString, String: strings.TrimSuffix(line, "\r\n")}, nil
}

type integerParser struct{}

func (p integerParser) Parse(reader *TrackingBufReader) (RESPValue, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return RESPValue{}, err
	}
	num, err := strconv.ParseInt(strings.TrimSpace(line), 10, 64)
	if err != nil {
		return RESPValue{}, err
	}
	return RESPValue{Type: Integer, Integer: num}, nil
}

type arrayParser struct{}

func (p arrayParser) Parse(reader *TrackingBufReader) (RESPValue, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return RESPValue{}, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		return RESPValue{}, err
	}

	values := make([]RESPValue, 0, count)
	for i := 0; i < count; i++ {
		val, err := parseRESPValue(reader)
		if err != nil {
			return RESPValue{}, err
		}
		values = append(values, val)
	}

	return RESPValue{
		Type: Array,
		// This can be stored or passed externally
		String: fmt.Sprintf("Array of %d items", count), // Debug only
		Array:  values,
	}, nil
}

func parseRESPValue(reader *TrackingBufReader) (RESPValue, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return RESPValue{}, err
	}

	parser, ok := respParsers[RESPType(prefix)]
	if !ok {
		return RESPValue{}, fmt.Errorf("unsupported RESP type: %q", prefix)
	}

	return parser.Parse(reader)
}

type errorParser struct{}

func (p errorParser) Parse(reader *TrackingBufReader) (RESPValue, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return RESPValue{}, err
	}
	return RESPValue{Type: Error, String: strings.TrimSuffix(line, "\r\n")}, nil
}

type bulkStringParser struct{}

func (p bulkStringParser) Parse(reader *TrackingBufReader) (RESPValue, error) {
	lenLine, err := reader.ReadString('\n')
	if err != nil {
		return RESPValue{}, err
	}
	length, err := strconv.Atoi(strings.TrimSpace(lenLine))
	if err != nil {
		return RESPValue{}, err
	}
	if length == -1 {
		return RESPValue{Type: BulkString, String: ""}, nil // or nil if you want to support NULL bulk
	}

	buf := make([]byte, length+2) // includes \r\n
	if _, err := io.ReadFull(reader, buf); err != nil {
		return RESPValue{}, err
	}

	return RESPValue{
		Type:   BulkString,
		String: string(buf[:length]),
	}, nil
}
