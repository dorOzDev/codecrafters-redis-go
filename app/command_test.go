package main

import (
	"fmt"
	"sync"
	"testing"
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

func TestSetGetCommand_Execute(t *testing.T) {
	ResetStore()
	setCmd := NewSetCommand([]RESPValue{
		{Type: BulkString, String: CommandSET},
		{Type: BulkString, String: "mykey"},
		{Type: BulkString, String: "myval"},
	})
	setResp := setCmd.Execute()
	if setResp.Type != SimpleString || setResp.String != "OK" {
		t.Errorf("SET expected OK, got %v", setResp)
	}

	// GET mykey
	getCmd := NewGetCommand([]RESPValue{
		{Type: BulkString, String: CommandGET},
		{Type: BulkString, String: "mykey"},
	})
	getResp := getCmd.Execute()
	if getResp.Type != BulkString || getResp.String != "myval" {
		t.Errorf("GET expected myval, got %v", getResp)
	}
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
