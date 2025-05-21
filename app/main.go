package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

const PORT_DEFUALT = "6379"

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	port, isExists := GetFlagValue(Port)
	if !isExists {
		port = PORT_DEFUALT
		fmt.Println("no port was set by the user using the default", PORT_DEFUALT)
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
