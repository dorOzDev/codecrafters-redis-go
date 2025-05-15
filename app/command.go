package main

import (
	"fmt"
	"strings"
)

const (
	CommandPING = "PING"
	CommandECHO = "ECHO"
	CommandSET  = "SET"
	CommandGET  = "GET"
)

type RESPCommand interface {
	Name() string
	Args() []RESPValue
	Execute() RESPValue
}

type PingCommand struct {
	values []RESPValue
}

func (p *PingCommand) Name() string      { return CommandPING }
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

func (e *EchoCommand) Name() string      { return CommandECHO }
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

type SetCommand struct {
	values []RESPValue
}

func (s *SetCommand) Name() string      { return CommandSET }
func (s *SetCommand) Args() []RESPValue { return s.values[1:] }
func (s *SetCommand) Execute() RESPValue {
	if len(s.values) < 3 {
		return RESPValue{Type: Error, String: "ERR wrong number of argument for SET commands"}
	}

	store.Set(s.values[1].String, s.values[2].String)
	return RESPValue{Type: SimpleString, String: "OK"}
}

type GetCommand struct {
	values []RESPValue
}

func (g *GetCommand) Name() string      { return CommandGET }
func (g *GetCommand) Args() []RESPValue { return g.values[1:] }
func (g *GetCommand) Execute() RESPValue {
	if len(g.values) < 2 {
		return RESPValue{Type: Error, String: "ERR wrong number of argument for GET commands"}
	}

	value, ok := store.Get(g.values[1].String)

	if !ok {
		return RESPValue{Type: BulkString, IsNil: true}
	}

	return RESPValue{Type: BulkString, String: value}
}

type CommandFactory func([]RESPValue) RESPCommand

func init() {
	commandRegistry[CommandPING] = NewPingCommand
	commandRegistry[CommandECHO] = NewEchoCommand
	commandRegistry[CommandSET] = NewSetCommand
	commandRegistry[CommandGET] = NewGetCommand
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

func NewSetCommand(values []RESPValue) RESPCommand {
	return &SetCommand{values: values}
}

func NewGetCommand(values []RESPValue) RESPCommand {
	return &GetCommand{values: values}
}
