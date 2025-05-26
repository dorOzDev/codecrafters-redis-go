package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	handleReplicationIfConfigured()
	port := resolvePort()
	listener := startTCPListener(port)
	defer listener.Close()

	loadInitialDatabase()

	acceptConnections(listener)
}

func conntectToMaster(host, port string) (net.Conn, error) {
	addr := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}
	fmt.Println("Connected to master at", addr)
	return conn, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	fmt.Println("New connection")

	for {
		val, err := parseRESPValue(reader)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("Received:", val.Type)

		if val.Type != Array {
			fmt.Fprintln(conn, "-ERR expected array")
			return
		}

		cmd, err := ParseRESPCommandFromArray(val.Array)
		if err != nil {
			fmt.Fprintf(conn, "-ERR %v\r\n", err)
			return
		}

		response := cmd.Execute()
		serializedData, err := response.Serialize()
		if err != nil {
			log.Println("failed to write response:", err)
			return
		}

		conn.Write(serializedData)
	}
}

func handleReplicationIfConfigured() {
	replicaOf, exists := GetFlagValue(FlagReplicaof)
	if !exists {
		return
	}

	fmt.Println("Setting replicaof:", replicaOf)
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
	defer conn.Close()

	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		log.Printf("Failed to send PING to master: %s, error: %q", replicaOf, err)
	}
}

func resolvePort() string {
	port, exists := GetFlagValue(FlagPort)
	if !exists {
		fmt.Println("No port specified. Using default:", PORT_DEFUALT)
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
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Error accepting connection: %v", err)
		}
		go handleConnection(conn)
	}
}
