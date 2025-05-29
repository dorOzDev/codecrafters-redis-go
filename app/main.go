package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	log.Println("Logs from your program will appear here!")
	port := resolvePort()

	log.Println("starting tcp server on port: ", port)
	listener := startTCPListener(port)
	defer listener.Close()
	handler, err := CreateConnectionHandler(listener, port)
	if err != nil {
		log.Fatalf("failed to create a new connection handler: %v", err)
		return
	}
	defer handler.Close()
	err = handler.HandleConnection()
	if err != nil {
		log.Fatalf("connection handler failed: %v", err)
		return
	}
}

func startTCPListener(port string) net.Listener {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		log.Fatalf("Failed to bind to port %s: %v", port, err)
	}
	return l
}

func resolvePort() string {
	port, exists := GetFlagValue(FlagPort)
	if !exists {
		log.Println("No port specified. Using default:", PORT_DEFUALT)
		return PORT_DEFUALT
	}
	return port
}
