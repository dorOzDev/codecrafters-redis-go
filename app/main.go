package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func main() {
	log.Println("Logs from your program will appear here!")

	port := resolvePort()
	listener := startTCPListener(port)
	log.Println("starting tcp server on port: ", port)
	defer listener.Close()

	loadInitialDatabase()

	handleReplicationIfConfigured()
	acceptConnections(listener)
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

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	log.Println("New connection")

	var isReplica bool
	defer func() {
		if !isReplica {
			log.Println("closing connection")
			conn.Close()
		}
	}()
	for {
		val, err := parseRESPValue(reader)
		if err != nil {
			log.Println(err)
			return
		}

		log.Println("Received:", val.Type)

		if val.Type != Array {
			fmt.Fprintln(conn, "-ERR expected array")
			return
		}

		log.Println("parsing command")
		cmd, err := ParseRESPCommandFromArray(val.Array)
		if err != nil {
			fmt.Fprintf(conn, "-ERR %v\r\n", err)
			return
		}

		log.Println("executing command: ", cmd)
		response := cmd.Execute()
		serializedData, err := response.Serialize()
		if err != nil {
			log.Println("failed to write response:", err)
			return
		}

		log.Println("writing respose to connection")
		conn.Write(serializedData)

		if postAction, ok := cmd.(PostCommandExecuteAction); ok {
			if err := postAction.HandlePostWrite(conn); err != nil {
				log.Printf("Post-Execution action failed: %v", err)
				return
			}
			isReplica = true
		}

		if !isReplica {
			if replicableCommand, ok := cmd.(ReplicableCommand); ok {
				if replicableCommand.ShouldReplicate() {
					log.Printf("replicating command to all replicas")
					broadcastToReplicas(RESPValue{Type: Array, Array: val.Array})
				}
			}
		}
	}
}

func handleReplicationIfConfigured() {
	replicaOf, exists := GetFlagValue(FlagReplicaof)
	if !exists {
		return
	}

	log.Println("Setting replicaof:", replicaOf)
	master := strings.Split(replicaOf, " ")
	if len(master) != 2 {
		log.Fatalf("Invalid replicaof format. Expected: <host> <port>")
	}
	masterHost, masterPort := master[0], master[1]

	conn, err := conntectToMaster(masterHost, masterPort)
	if err != nil {
		log.Printf("Unable to connect with master: %s, error: %q", replicaOf, err)
		return
	}

	localPort, _ := GetFlagValue(FlagPort)
	if err := performReplicationHandshake(conn, localPort); err != nil {
		log.Printf("Replication handshake with master failed: %v", err)
	}

	log.Println(conn.LocalAddr())
	log.Println(conn.RemoteAddr())
	go startReplicationReadLoop(conn)
}

func startReplicationReadLoop(conn net.Conn) {

	reader := bufio.NewReader(conn)
	for {
		val, err := parseRESPValue(reader)
		if err != nil {
			log.Printf("Replication read error: %v", err)
			return
		}

		log.Printf("Received replicated command: %+v", val)

		cmd, err := ParseRESPCommandFromArray(val.Array)
		if err != nil {
			log.Printf("Parse error: %v", err)
			continue
		}

		cmd.Execute() // Actually applies SET/DEL to the replica's store
	}
}

func performReplicationHandshake(conn net.Conn, localPort string) error {
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

	reader := bufio.NewReader(conn)
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
	return nil
}

func resolvePort() string {
	port, exists := GetFlagValue(FlagPort)
	if !exists {
		log.Println("No port specified. Using default:", PORT_DEFUALT)
		return PORT_DEFUALT
	}
	return port
}

func startTCPListener(port string) net.Listener {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		log.Fatalf("Failed to bind to port %s: %v", port, err)
	}
	return l
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
