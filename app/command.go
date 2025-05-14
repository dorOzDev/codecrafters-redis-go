package main

import (
	"fmt"
	"strings"
)

type RESPCommand interface {
	Name() string
	Args() []RESPValue
	Execute() RESPValue
}

type PingCommand struct {
	values []RESPValue
}

func (p *PingCommand) Name() string      { return "PING" }
func (p *PingCommand) Args() []RESPValue { return p.values[1:] }
func (p *PingCommand) Execute() RESPValue {
	return RESPValue{
		Type:   SimpleString,
		String: "PONG",
	}
}

type EchoCommand struct {
	values []RESPValue
}

func (e *EchoCommand) Name() string      { return "ECHO" }
func (e *EchoCommand) Args() []RESPValue { return e.values[1:] }
func (e *EchoCommand) Execute() RESPValue {
	if len(e.values) < 2 {
		return RESPValue{Type: Error, String: "ERR missing argument for ECHO"}
	}
	return RESPValue{
		Type:   SimpleString,
		String: e.values[1].String,
	}
}

type CommandFactory func([]RESPValue) RESPCommand

func init() {
	commandRegistry["PING"] = NewPingCommand
	commandRegistry["ECHO"] = NewEchoCommand
}

var commandRegistry = map[string]CommandFactory{}

func ParseRESPCommandFromArray(values []RESPValue) (RESPCommand, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("empty RESP array")
	}
	if values[0].Type != BulkString {
		return nil, fmt.Errorf("command name must be bulk string")
	}

	name := strings.ToUpper(values[0].String)
	factory, ok := commandRegistry[name]
	if !ok {
		return nil, fmt.Errorf("unknown command: %s", name)
	}

	return factory(values), nil
}

func NewPingCommand(values []RESPValue) RESPCommand {
	return &PingCommand{values: values}
}

func NewEchoCommand(values []RESPValue) RESPCommand {
	return &EchoCommand{values: values}
}
