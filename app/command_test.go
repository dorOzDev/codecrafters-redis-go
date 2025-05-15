package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPingCommand_Execute(t *testing.T) {
	cmd := NewPingCommand([]RESPValue{{Type: BulkString, String: CommandPING}})
	resp := cmd.Execute()

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
	resp := cmd.Execute()

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

	resp := cmd.Execute()

	assert.Equal(t, "OK", resp.String)

	entry, ok := store.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", entry.Val)
	assert.Equal(t, InfiniteTTL, entry.TTL)
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

	resp := cmd.Execute()
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

	resp := cmd.Execute()
	assert.Equal(t, Error, resp.Type)
}

func TestGetCommand_MissingKey(t *testing.T) {
	ResetStore()
	getCmd := NewGetCommand([]RESPValue{
		{Type: BulkString, String: CommandGET},
		{Type: BulkString, String: "missing"},
	})
	resp := getCmd.Execute()

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
				resp := setCmd.Execute()
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
				resp := getCmd.Execute()

				// Only validate non-nil values
				if !resp.IsNil && resp.Type != BulkString {
					t.Errorf("Get returned unexpected type for %s: %+v", key, resp)
				}
			}
		}(i)
	}

	wg.Wait()
}
