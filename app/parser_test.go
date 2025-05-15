package main

import (
	"bufio"
	"strings"
	"testing"
)

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
			name:  "Array - ECHO command",
			input: "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
			expected: RESPValue{
				Type: Array,
				Array: []RESPValue{
					{Type: BulkString, String: CommandECHO},
					{Type: BulkString, String: "hey"},
				},
			},
		},
		{
			name:  "Array - SET command with PX",
			input: "*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n100\r\n",
			expected: RESPValue{
				Type: Array,
				Array: []RESPValue{
					{Type: BulkString, String: CommandSET},
					{Type: BulkString, String: "mykey"},
					{Type: BulkString, String: "value"},
					{Type: BulkString, String: "PX"},
					{Type: BulkString, String: "100"},
				},
			},
		},

		{
			name:  "Array - GET command",
			input: "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n",
			expected: RESPValue{
				Type: Array,
				Array: []RESPValue{
					{Type: BulkString, String: CommandGET},
					{Type: BulkString, String: "mykey"},
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

			if !EqualRESPValue(got, test.expected) {
				t.Errorf("Mismatch:\nGot: %+v\nExpected: %+v", got, test.expected)
			}
		})
	}
}
