package main

import (
	"bufio"
	"strings"
	"testing"
)

func equalRESPValue(a, b RESPValue) bool {
	if a.Type != b.Type {
		return false
	}

	switch a.Type {
	case SimpleString, Error, BulkString:
		return a.String == b.String
	case Integer:
		return a.Integer == b.Integer
	case Array:
		if len(a.Array) != len(b.Array) {
			return false
		}
		for i := range a.Array {
			if !equalRESPValue(a.Array[i], b.Array[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func TestParseRESPValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected RESPValue
	}{
		{
			name:  "Simple String",
			input: "+OK\r\n",
			expected: RESPValue{
				Type:   SimpleString,
				String: "OK",
			},
		},
		{
			name:  "Error",
			input: "-ERR unknown command\r\n",
			expected: RESPValue{
				Type:   Error,
				String: "ERR unknown command",
			},
		},
		{
			name:  "Integer",
			input: ":1000\r\n",
			expected: RESPValue{
				Type:    Integer,
				Integer: 1000,
			},
		},
		{
			name:  "Bulk String",
			input: "$5\r\nhello\r\n",
			expected: RESPValue{
				Type:   BulkString,
				String: "hello",
			},
		},
		{
			name:  "Array of Bulk Strings",
			input: "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
			expected: RESPValue{
				Type: Array,
				Array: []RESPValue{
					{Type: BulkString, String: "ECHO"},
					{Type: BulkString, String: "hey"},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(test.input))
			got, err := parseRESPValue(reader)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !equalRESPValue(got, test.expected) {
				t.Errorf("Mismatch:\nGot: %+v\nExpected: %+v", got, test.expected)
			}
		})
	}
}
