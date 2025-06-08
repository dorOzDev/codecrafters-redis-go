package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type ConnectionHandler interface {
	HandleConnection() error
	Close()
}

type MasterConnectionHandler struct {
	port     string
	listener net.Listener
}

func (handler MasterConnectionHandler) HandleConnection() error {
	loadInitialDatabase()
	acceptConnections(handler.listener)

	return nil
}

func (handler MasterConnectionHandler) Close() {
	handler.listener.Close()
}

type ReplicaConnectionHandler struct {
	masterHost, masterPort, port string
	readyToServe                 *atomic.Bool
	listener                     net.Listener
}

func (handler ReplicaConnectionHandler) HandleConnection() error {
	loadInitialDatabase()
	err := handler.handleReplication()
	if err != nil {
		return err
	}

	acceptConnections(handler.listener)

	return nil
}

func (handler ReplicaConnectionHandler) Close() {
	handler.listener.Close()
}

func CreateConnectionHandler(listener net.Listener, port string) (ConnectionHandler, error) {
	val, exists := GetFlagValue(FlagReplicaof)
	if exists {
		log.Println("a new replica connection: ", val)
		master := strings.Split(val, " ")
		if len(master) != 2 {
			return nil, fmt.Errorf("invalid reokucaif format. expected <host> <port>")
		}

		return ReplicaConnectionHandler{
			masterHost:   master[0],
			masterPort:   master[1],
			readyToServe: new(atomic.Bool),
			port:         port,
			listener:     listener,
		}, nil
	} else {
		log.Println("a new master connection")
		return MasterConnectionHandler{
			port:     port,
			listener: listener,
		}, nil
	}
}

func conntectToMaster(host, port string) (net.Conn, error) {
	addr := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}
	log.Println("Connected to master at", addr)
	return conn, nil
}

func handleConnection(conn net.Conn) (shouldClose bool) {
	shouldClose = true
	reader := NewTrackingBufReader(conn)
	log.Println("New connection")

	defer func() {
		if shouldClose {
			log.Println("closing connection")
			conn.Close()
		} else {
			log.Println("connection handed off to another routine")
		}
	}()

	for {
		cmd, respVal, err := parseRESPCommand(reader)
		if err != nil {
			fmt.Fprintf(conn, "-ERR %v\r\n", err)
			return
		}

		afterCommadFunc := func(cmd RESPCommand, commandResult RESPValue) error {
			err := writeSerializedDataToConnection(conn, commandResult)
			if err != nil {
				log.Println("failed to write serialized data to connection: ", err)
				return err
			}
			if ka, ok := cmd.(KeepAliveCommand); ok && ka.KeepsConnectionAlive() {
				log.Println("Command takes over connection lifecycle")
				shouldClose = false
				return err
			}

			if replicableCommand, ok := cmd.(WriteCommand); ok {
				if replicableCommand.ShouldReplicate() {
					log.Println("Replicating command to all replicas")
					broadcastToReplicas(respVal)
				}
			}
			if postAction, ok := cmd.(PostCommandExecuteAction); ok {
				if err := postAction.HandlePostWrite(conn); err != nil {
					log.Printf("Post-Execution action failed: %v", err)
					return err
				}
			}
			return nil
		}

		executeRespCommand(cmd, CommandContext{Conn: conn}, &ExcecuteCommandHook{AfterCommndFunc: afterCommadFunc})
	}
}

type ExcecuteCommandHook struct {
	BeforeParseFunc func() error
	AfterCommndFunc func(cmd RESPCommand, commnandResult RESPValue) error
}

func executeRespCommand(cmd RESPCommand, commandContext CommandContext, commandExecutionHook *ExcecuteCommandHook) {
	log.Println("executing command:", cmd)
	response := cmd.Execute(commandContext)

	if commandExecutionHook != nil && commandExecutionHook.AfterCommndFunc != nil {
		log.Println("runnin after command executed code")
		commandExecutionHook.AfterCommndFunc(cmd, response)
	}
}

func parseRESPCommand(reader *TrackingBufReader) (RESPCommand, RESPValue, error) {
	val, err := parseRESPValue(reader)
	if err != nil {
		return nil, RESPValue{}, fmt.Errorf("parseRESPValue error: %w", err)
	}

	if val.Type != Array {
		return nil, val, fmt.Errorf("invalid RESP type: %v", val.Type)
	}

	cmd, err := ParseRESPCommandFromArray(val.Array)
	if err != nil {
		return nil, val, fmt.Errorf("command parse error: %w", err)
	}

	return cmd, val, nil
}

func (handler *ReplicaConnectionHandler) handleReplication() error {
	conn, err := conntectToMaster(handler.masterHost, handler.masterPort)
	if err != nil {
		log.Printf("Unable to connect with master:, error: %q", err)
		return err
	}

	trackBufReader := NewTrackingBufReader(conn)

	if err := handler.performReplicationHandshake(conn, handler.port, trackBufReader.Reader); err != nil {
		log.Printf("Replication handshake with master failed: %v", err)
		return err
	}
	stats := &ReplicaTrackingBytes{}
	go handler.startReplicationRead(conn, trackBufReader, stats)
	return nil
}

func (handler *ReplicaConnectionHandler) startReplicationRead(conn net.Conn, reader *TrackingBufReader, replicaStats *ReplicaTrackingBytes) {
	for !handler.readyToServe.Load() {
		log.Println("[REPLICA] Not ready yet, blocking client")
		time.Sleep(10 * time.Millisecond)
	}
	log.Println("[REPLICA] accepting connections")

	for {
		cmd, _, err := parseRESPCommand(reader)
		if err != nil {
			fmt.Fprintf(conn, "-ERR %v\r\n", err)
			return
		}

		afterCommadFunc := func(cmd RESPCommand, commandResult RESPValue) error {
			if sendResponseToMasterCommand, ok := cmd.(SendResonseToMaster); ok && sendResponseToMasterCommand.ShouldResponseBackToMaster() {
				log.Printf("[REPLICA] writing response to master")
				err = writeSerializedDataToConnection(conn, commandResult)
				if err != nil {
					log.Println("fialed to write serialized data to connection: ", err)
					return err
				}
			}
			reader.FlushTo(replicaStats)
			return nil
		}

		executeRespCommand(cmd, CommandContext{Conn: conn, replicaStats: replicaStats}, &ExcecuteCommandHook{AfterCommndFunc: afterCommadFunc})
	}
}

func (handler *ReplicaConnectionHandler) performReplicationHandshake(conn net.Conn, localPort string, reader *bufio.Reader) error {
	log.Println("Replica: sending ping")
	if err := sendPing(conn); err != nil {
		return fmt.Errorf("PING failed: %w", err)
	}

	log.Println("Replica: sending listening-port")
	if err := sendReplConf(conn, "listening-port", localPort); err != nil {
		return fmt.Errorf("REPLCONF listening-port failed: %w", err)
	}

	log.Println("Replica: sending cap pysnc2")
	if err := sendReplConf(conn, "capa", "psync2"); err != nil {
		return fmt.Errorf("REPLCONF capa failed: %w", err)
	}

	log.Println("Replica: sending Psync")
	if err := sendPsync(conn); err != nil {
		return fmt.Errorf("PSYNC failed: %w", err)
	}
	log.Println("Replica: reading bulk header")

	bulkHeader, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read bulk string header: %w", err)
	}
	log.Printf("Bulk Header: %s", bulkHeader)

	if !strings.HasPrefix(bulkHeader, "$") {
		return fmt.Errorf("expected RESP bulk string, got: %s", bulkHeader)
	}

	sizeStr := strings.TrimPrefix(bulkHeader, "$")
	sizeStr = strings.TrimSpace(sizeStr)
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		return fmt.Errorf("invalid bulk size: %w", err)
	}

	log.Printf("Reading %d bytes of RDB data from master", size)
	limitedReader := io.LimitReader(reader, int64(size))

	err = parseRDB(limitedReader, store)
	if err != nil {
		return fmt.Errorf("failed to parse RDB data: %w", err)
	}

	log.Println("Replica: RDB sync complete")
	handler.readyToServe.Store(true)

	return nil
}

func loadInitialDatabase() {
	dir, _ := GetFlagValue(FlagDir)
	dbFileName, _ := GetFlagValue(FlagDbFilename)

	if err := LoadRDBFile(dir, dbFileName, store); err != nil {
		log.Printf("Error loading RDB file: %v", err)
	}
}

func acceptConnections(listener net.Listener) {
	log.Println("accepting connections")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Error accepting connection: %v", err)
		}
		go handleConnection(conn)
	}
}

func writeSerializedDataToConnection(conn net.Conn, response RESPValue) error {

	serializedData, err := response.Serialize()
	if err != nil {
		log.Println("failed to serialize response:", err)
		return err
	}

	if _, err := conn.Write(serializedData); err != nil {
		log.Println("failed to write response:", err)
		return err
	}

	return nil
}

type KeepAliveCommand interface {
	KeepsConnectionAlive() bool
}

type SendResonseToMaster interface {
	ShouldResponseBackToMaster() bool
}
