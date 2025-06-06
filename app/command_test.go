package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPingCommand_Execute(t *testing.T) {
	cmd := NewPingCommand([]RESPValue{{Type: BulkString, String: CommandPING}})
	resp := cmd.Execute(CommandContext{})

	want := RESPValue{Type: SimpleString, String: "PONG"}
	if !EqualRESPValue(resp, want) {
		t.Errorf("PING expected %v, got %v", want, resp)
	}
}

func TestEchoCommand_Execute(t *testing.T) {
	cmd := NewEchoCommand([]RESPValue{
		{Type: BulkString, String: CommandECHO},
		{Type: BulkString, String: "hello"},
	})
	resp := cmd.Execute(CommandContext{})

	want := RESPValue{Type: SimpleString, String: "hello"}
	if !EqualRESPValue(resp, want) {
		t.Errorf("ECHO expected %v, got %v", want, resp)
	}
}

func TestSetCommand_NoTTL(t *testing.T) {
	ResetStore()

	cmd := &SetCommand{
		values: []RESPValue{
			{Type: BulkString, String: "SET"},
			{Type: BulkString, String: "foo"},
			{Type: BulkString, String: "bar"},
		},
	}

	resp := cmd.Execute(CommandContext{})

	assert.Equal(t, "OK", resp.String)

	entry, ok := store.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", entry.Val)
	assert.Nil(t, entry.ExpireAt)
}

func TestSetCommand_WithTTL(t *testing.T) {
	ResetStore()

	cmd := &SetCommand{
		values: []RESPValue{
			{Type: BulkString, String: "SET"},
			{Type: BulkString, String: "expiring"},
			{Type: BulkString, String: "soon"},
			{Type: BulkString, String: "PX"},
			{Type: BulkString, String: "50"},
		},
	}

	resp := cmd.Execute(CommandContext{})
	assert.Equal(t, "OK", resp.String)

	time.Sleep(60 * time.Millisecond)

	_, ok := store.Get("expiring")
	assert.False(t, ok)
}

func TestSetCommand_InvalidPX(t *testing.T) {
	tests := []string{"notanumber", "-1"}
	for _, ttl := range tests {
		t.Run("PX="+ttl, func(t *testing.T) {
			ResetStore()
			testInvalidPX(t, ttl)
		})
	}
}

func testInvalidPX(t *testing.T, ttl string) {
	cmd := &SetCommand{
		values: []RESPValue{
			{Type: BulkString, String: "SET"},
			{Type: BulkString, String: "x"},
			{Type: BulkString, String: "y"},
			{Type: BulkString, String: "PX"},
			{Type: BulkString, String: ttl},
		},
	}

	resp := cmd.Execute(CommandContext{})
	assert.Equal(t, Error, resp.Type)
}

func TestGetCommand_MissingKey(t *testing.T) {
	ResetStore()
	getCmd := NewGetCommand([]RESPValue{
		{Type: BulkString, String: CommandGET},
		{Type: BulkString, String: "missing"},
	})
	resp := getCmd.Execute(CommandContext{})

	if !resp.IsNil || resp.Type != BulkString {
		t.Errorf("GET missing key should return nil bulk, got %+v", resp)
	}
}

func TestSetGetCommands_ThreadSafety(t *testing.T) {
	ResetStore() // clears the global store

	const workers = 100
	const iterations = 100

	var wg sync.WaitGroup

	// Concurrent writers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)

			for j := 0; j < iterations; j++ {
				setCmd := NewSetCommand([]RESPValue{
					{Type: BulkString, String: CommandSET},
					{Type: BulkString, String: key},
					{Type: BulkString, String: fmt.Sprintf("val-%d", j)},
				})
				resp := setCmd.Execute(CommandContext{})
				if resp.Type != SimpleString || resp.String != "OK" {
					t.Errorf("Set failed for %s: %v", key, resp)
				}
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)

			for j := 0; j < iterations; j++ {
				getCmd := NewGetCommand([]RESPValue{
					{Type: BulkString, String: CommandGET},
					{Type: BulkString, String: key},
				})
				resp := getCmd.Execute(CommandContext{})

				// Only validate non-nil values
				if !resp.IsNil && resp.Type != BulkString {
					t.Errorf("Get returned unexpected type for %s: %+v", key, resp)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestConfigCommand(t *testing.T) {
	ResetStore()
	originalArgs := os.Args
	defer func() { os.Args = originalArgs }()

	const (
		TEST_DIR     = "/tmp/test-dir"
		DB_FILE_NAME = "test.rdb"
	)

	os.Args = []string{
		"your_program", FlagDir, TEST_DIR, FlagDbFilename, DB_FILE_NAME,
	}

	tests := []struct {
		name          string
		flag          string
		expectedKey   string
		expectedValue string
	}{
		{"Get dir", FlagDir[2:], FlagDir[2:], TEST_DIR},
		{"Get dbFilename", FlagDbFilename[2:], FlagDbFilename[2:], DB_FILE_NAME},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := NewConfigCommand([]RESPValue{
				{Type: BulkString, String: CommandCONFIG},
				{Type: BulkString, String: "GET"},
				{Type: BulkString, String: test.flag},
			})

			resp := command.Execute(CommandContext{})
			assert.Equal(t, Array, resp.Type)
			assert.Len(t, resp.Array, 2)
			assert.Equal(t, test.expectedKey, resp.Array[0].String)
			assert.Equal(t, test.expectedValue, resp.Array[1].String)
		})
	}
}
