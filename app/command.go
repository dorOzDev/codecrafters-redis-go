package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	CommandPING   = "PING"
	CommandECHO   = "ECHO"
	CommandSET    = "SET"
	CommandGET    = "GET"
	CommandCONFIG = "CONFIG"
	CommandKEYS   = "KEYS"
	CommandINFO   = "INFO"
	CommandREPL   = "REPLCONF"
	CommandPSYNC  = "PSYNC"
	CommandWAIT   = "WAIT"
)

type RESPCommand interface {
	Name() string
	Args() []RESPValue
	Execute(context CommandContext) RESPValue
}

type PingCommand struct {
	values []RESPValue
}

func (p *PingCommand) Name() string      { return CommandPING }
func (p *PingCommand) Args() []RESPValue { return p.values[1:] }
func (p *PingCommand) Execute(context CommandContext) RESPValue {
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
func (e *EchoCommand) Execute(context CommandContext) RESPValue {
	if len(e.values) < 2 {
		return RESPValue{Type: Error, String: "ERR missing argument for ECHO"}
	}
	return RESPValue{
		Type:   SimpleString,
		String: e.values[1].String,
	}
}

type SetCommand struct {
	BaseWriteCommand
	values []RESPValue
}

func (s *SetCommand) Name() string      { return CommandSET }
func (s *SetCommand) Args() []RESPValue { return s.values[1:] }
func (s *SetCommand) Execute(context CommandContext) RESPValue {
	log.Println("in execute set command")
	if len(s.values) < 3 {
		return RESPValue{Type: Error, String: "ERR wrong number of argument for SET commands"}
	}

	key := s.values[1].String
	value := s.values[2].String
	var expireAt *int64 = nil

	if len(s.values) >= 5 && strings.ToUpper(s.values[3].String) == "PX" {
		ttlMillis, err := strconv.Atoi(s.values[4].String)
		if err != nil || ttlMillis < 0 {
			return RESPValue{Type: Error, String: "ERR PX value must be a non-negative integer"}
		}
		exp := time.Now().UnixMilli() + int64(ttlMillis)
		expireAt = &exp
	}

	log.Printf("setting key: %s, value: %s", key, value)
	store.Set(key, Entry{
		Val:      value,
		ExpireAt: expireAt,
	})

	return RESPValue{Type: SimpleString, String: "OK"}
}

type GetCommand struct {
	values []RESPValue
}

func (g *GetCommand) Name() string      { return CommandGET }
func (g *GetCommand) Args() []RESPValue { return g.values[1:] }
func (g *GetCommand) Execute(context CommandContext) RESPValue {
	if len(g.values) < 2 {
		return RESPValue{Type: Error, String: "ERR wrong number of argument for GET commands"}
	}

	value, ok := store.Get(g.values[1].String)

	if !ok {
		log.Printf("on get command, key %s, doesn't exists.\n", g.values[1].String)
		return RESPValue{Type: BulkString, IsNil: true}
	}

	log.Printf("on get command, key: %s, value: %s", g.values[1].String, value.Val)
	return RESPValue{Type: BulkString, String: value.Val}
}

type ConfigCommand struct {
	values []RESPValue
}

func (c *ConfigCommand) Name() string      { return CommandCONFIG }
func (c *ConfigCommand) Args() []RESPValue { return c.values[1:] }
func (c *ConfigCommand) Execute(context CommandContext) RESPValue {
	if len(c.values) < 3 {
		return RESPValue{Type: Error, String: "ERR wrong number of arguments for CONFIG GET"}
	}

	argName := c.values[2]

	argValue, exists := GetFlagValue(argName.String)

	if !exists {
		return RESPValue{Type: Array, Array: []RESPValue{}}
	}

	var responseArr []RESPValue
	responseArr = append(responseArr, RESPValue{Type: BulkString, String: argName.String})
	responseArr = append(responseArr, RESPValue{Type: BulkString, String: argValue})

	return RESPValue{Type: Array, Array: responseArr}
}

type KeysCommand struct {
	values []RESPValue
}

func (*KeysCommand) Name() string        { return CommandKEYS }
func (k *KeysCommand) Args() []RESPValue { return k.values[1:] }
func (k *KeysCommand) Execute(context CommandContext) RESPValue {
	if len(k.values) != 2 {
		return RESPValue{Type: Error, String: "ERR wrong number of arguments for KEYS command"}
	}

	pattern := k.values[1].String
	if pattern != "*" {
		return RESPValue{Type: Error, String: "ERR only KEYS * is supported"}
	}

	keys := store.Keys()

	respKeys := make([]RESPValue, 0, len(keys))
	for _, key := range keys {
		respKeys = append(respKeys, RESPValue{Type: BulkString, String: key})
	}

	return RESPValue{Type: Array, Array: respKeys}
}

type InfoCommand struct {
	values []RESPValue
}

func (*InfoCommand) Name() string        { return CommandINFO }
func (i *InfoCommand) Args() []RESPValue { return i.values[1:] }
func (i *InfoCommand) Execute(context CommandContext) RESPValue {
	var args []string
	for _, arg := range i.values[1:] {
		args = append(args, arg.String)
	}

	sections := getSectionsByNames(args...)

	var stringBuilder strings.Builder
	for _, section := range sections {
		stringBuilder.Write([]byte(section.GetInfo()))
	}

	return RESPValue{Type: BulkString, String: strings.TrimSpace(stringBuilder.String())}
}

type ReplConfCommand struct {
	values []RESPValue
}

func (*ReplConfCommand) Name() string        { return CommandREPL }
func (r *ReplConfCommand) Args() []RESPValue { return r.values[1:] }

func (r *ReplConfCommand) Execute(context CommandContext) RESPValue {
	args := r.Args()
	if len(args) == 0 {
		return RESPValue{Type: Error, String: "ERR missing arguments for REPLCONF"}
	}

	subCmd := strings.ToLower(args[0].String)

	if subCmd == "getack" {
		if len(args) != 2 || args[1].String != "*" {
			return RESPValue{Type: Error, String: "ERR REPLCONF GETACK requires '*'"}
		}

		log.Println("Received REPLCONF GETACK *")

		offset := context.replicaStats.BytesRead

		return RESPValue{
			Type: Array,
			Array: []RESPValue{
				{Type: BulkString, String: CommandREPL},
				{Type: BulkString, String: "ACK"},
				{Type: BulkString, String: strconv.FormatUint(offset, 10)},
			},
		}
	} else if subCmd == "ack" {
		if len(r.Args()) != 3 {
			return RESPValue{Type: Error, String: "ERR wrong number of arguments for REPLCONF ACK"}
		}

		offsetStr := r.Args()[2].String
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return RESPValue{Type: Error, String: "ERR invalid offset in REPLCONF ACK"}
		}

		UpdateReplicaAckOffsetByConn(context.Conn, offset)
		return RESPValue{Type: SimpleString, String: "OK"}
	}

	// Default key-value case
	if len(args)%2 != 0 {
		return RESPValue{Type: Error, String: "ERR wrong number of arguments for REPLCONF"}
	}

	for i := 0; i < len(args); i += 2 {
		key := strings.ToLower(args[i].String)
		value := args[i+1].String
		log.Printf("REPLCONF: %s = %s\n", key, value)
	}

	return RESPValue{Type: SimpleString, String: "OK"}
}

type PsyncCommand struct {
	values []RESPValue
}

func (*PsyncCommand) Name() string        { return CommandPSYNC }
func (p *PsyncCommand) Args() []RESPValue { return p.values[1:] }

func (p *PsyncCommand) Execute(context CommandContext) RESPValue {
	args := p.Args()
	if len(args) != 2 {
		return RESPValue{Type: Error, String: "ERR wrong number of arguments for PSYNC"}
	}

	replicationID := args[0].String
	offset := args[1].String

	log.Printf("PSYNC received: replicationID=%s, offset=%s\n", replicationID, offset)

	// Hardcoded full resync response
	return RESPValue{
		Type:   SimpleString,
		String: fmt.Sprintf("FULLRESYNC %s %d", GetMasterReplId(), 0),
	}
}

type WaitCommand struct {
	values []RESPValue
}

func (w *WaitCommand) Name() string {
	return CommandWAIT
}

func (w *WaitCommand) Args() []RESPValue {
	return w.values
}

func (w *WaitCommand) Execute(ctx CommandContext) RESPValue {
	if len(w.Args()) != 3 {
		return RESPValue{
			Type:   Error,
			String: "ERR wrong number of arguments for 'WAIT' command",
		}
	}

	numReplicas, err := strconv.Atoi(w.Args()[1].String)
	if err != nil || numReplicas < 0 {
		return RESPValue{Type: Error, String: "ERR invalid number of replicas"}
	}

	timeoutMs, err := strconv.Atoi(w.Args()[2].String)
	if err != nil || timeoutMs < 0 {
		return RESPValue{Type: Error, String: "ERR invalid timeout"}
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		acked := 0
		for _, replicateState := range GetAllConnectedReplicas() {
			if !replicateState.NeedsAck() {
				log.Println("[WAIT] acked command for replica: ", replicateState.Addr)
				acked++
				continue
			}
			// Still needs ACK — trigger GETACK
			go func(repicaState *ReplicaState) {
				if err := repicaState.SendAck(100 * time.Millisecond); err != nil {
					log.Printf("[WAIT] retry ack failed: %v", err)
				} else {
					log.Printf("expected bytes: %d, actual bytes: %d", repicaState.PendingOffset, repicaState.LastAckOffset)
				}
			}(replicateState)
		}

		if acked >= numReplicas {
			return RESPValue{
				Type:    Integer,
				Integer: int64(acked),
			}
		}

		if time.Now().After(deadline) {
			return RESPValue{
				Type:    Integer,
				Integer: int64(acked),
			}
		}

		<-ticker.C
	}
}

type CommandFactory func([]RESPValue) RESPCommand

func init() {
	commandRegistry[CommandPING] = NewPingCommand
	commandRegistry[CommandECHO] = NewEchoCommand
	commandRegistry[CommandSET] = NewSetCommand
	commandRegistry[CommandGET] = NewGetCommand
	commandRegistry[CommandCONFIG] = NewConfigCommand
	commandRegistry[CommandKEYS] = NewKeysCommand
	commandRegistry[CommandINFO] = NewInfoCommand
	commandRegistry[CommandREPL] = NewReplConfCommand
	commandRegistry[CommandPSYNC] = NewPsyncCommand
	commandRegistry[CommandWAIT] = NewCommandWait
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

func NewConfigCommand(values []RESPValue) RESPCommand {
	return &ConfigCommand{values: values}
}

func NewKeysCommand(values []RESPValue) RESPCommand {
	return &KeysCommand{values: values}
}

func NewInfoCommand(values []RESPValue) RESPCommand {
	return &InfoCommand{values: values}
}

func NewReplConfCommand(values []RESPValue) RESPCommand {
	return &ReplConfCommand{values: values}
}

func NewPsyncCommand(values []RESPValue) RESPCommand {
	return &PsyncCommand{values: values}
}

func NewCommandWait(values []RESPValue) RESPCommand {
	return &WaitCommand{values: values}
}

/**if any Post command action is required, the command can imlement this interface*/
type PostCommandExecuteAction interface {
	HandlePostWrite(conn net.Conn) error
}

func (*PsyncCommand) KeepsConnectionAlive() bool {
	return true
}

func (p *PsyncCommand) HandlePostWrite(conn net.Conn) error {
	err := streamRdbFileToReplica(conn)
	if err != nil {
		log.Printf("failed streaming rdb file to replica: %v", err)
		return err
	}

	log.Println("register a replica")
	registerReplica(conn)
	return nil
}

func streamRdbFileToReplica(conn net.Conn) error {
	rdbPath := getRDBPath()
	log.Println("open init rdb file from: ", rdbPath)

	var rdbReader io.Reader
	var rdbSize int64

	file, err := os.Open(rdbPath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist — generate minimal empty RDB on the fly
			log.Println("RDB file does not exist. Generating empty RDB.")
			emptyRDB := generateEmptyRDB()
			rdbReader = bytes.NewReader(emptyRDB)
			rdbSize = int64(len(emptyRDB))
		} else {
			log.Printf("Failed to open RDB file: %v", err)
			return err
		}
	} else {
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			log.Printf("Failed to stat RDB file: %v", err)
			return err
		}
		rdbReader = file
		rdbSize = info.Size()
	}

	// Send RESP bulk string header
	header := fmt.Sprintf("$%d\r\n", rdbSize)
	if _, err := conn.Write([]byte(header)); err != nil {
		log.Printf("Failed to send RDB header: %v", err)
		return err
	}

	// Stream the RDB content
	if _, err := io.Copy(conn, rdbReader); err != nil {
		log.Printf("Failed to stream RDB content: %v", err)
		return err
	}
	return nil
}

func generateEmptyRDB() []byte {
	var buf bytes.Buffer
	buf.WriteString("REDIS0012")
	buf.WriteByte(0xFF)
	buf.Write(make([]byte, 8))

	return buf.Bytes()
}

func getRDBPath() string {
	exePath, err := os.Executable()
	if err != nil {
		log.Fatalf("Failed to get executable path: %v", err)
	}
	exeDir := filepath.Dir(exePath)
	rdbPath := filepath.Join(exeDir, "..", "data", "empty.rdb")
	rdbPath = filepath.Clean(rdbPath)
	log.Printf("getRDBPath() -> exe = %q, resolved = %q", exePath, rdbPath)
	return rdbPath
}

type WriteCommand interface {
	RESPCommand
	ShouldReplicate() bool
}

func (s *SetCommand) ShouldReplicate() bool {
	return true
}

func (r *ReplConfCommand) ShouldResponseBackToMaster() bool {
	return true
}

type CommandContext struct {
	Conn         net.Conn
	replicaStats *ReplicaTrackingBytes
}

type BaseWriteCommand struct{}

func (BaseWriteCommand) IsWriteCommand() bool {
	return true
}
