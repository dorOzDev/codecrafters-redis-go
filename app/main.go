package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

const PORT_DEFUALT = "6379"

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	port, isExists := GetFlagValue(FlagPort)
	if !isExists {
		port = PORT_DEFUALT
		fmt.Println("no port was set by the user using the default", PORT_DEFUALT)
	}

	replicaOf, isExists := GetFlagValue(FlagReplicaof)
	if isExists {
		fmt.Println("setting replicateof: ", replicaOf)
		master := strings.Split(replicaOf, " ")
		if len(master) != 2 {
			panic(fmt.Errorf("master is expected to be of length two consist of host and port space sapareted. i.e <host> <port>"))
		}
		host := master[0]
		port := master[1]

		conntectToMaster(host, port)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}

	dir, _ := GetFlagValue(FlagDir)
	dbFileName, _ := GetFlagValue(FlagDbFilename)
	err = LoadRDBFile(dir, dbFileName, store)

	if err != nil {
		fmt.Println("Error loading RDB file: ", err.Error())
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
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
