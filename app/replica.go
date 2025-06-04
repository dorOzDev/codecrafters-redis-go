package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	replicaMu         sync.RWMutex
	connectedReplicas = make(map[net.Conn]*Replica)
)

func registerReplica(conn net.Conn) {
	replicaMu.Lock()
	defer replicaMu.Unlock()

	connectedReplicas[conn] = &Replica{
		Conn: conn,
		Addr: conn.RemoteAddr().String(),
	}

	log.Printf("Registered replica: %s\n", conn.RemoteAddr().String())
	monitorReplicaConnection(conn)
}

func monitorReplicaConnection(conn net.Conn) {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			_, err := sendAckToReplica(conn)
			if err != nil {
				log.Printf("Replica %v is unreachable (ping failed): %v", conn.RemoteAddr(), err)
				unregisterReplica(conn)
				return
			}
		}
	}()
}

func unregisterReplica(conn net.Conn) {
	replicaMu.Lock()
	defer replicaMu.Unlock()

	if r, ok := connectedReplicas[conn]; ok {
		log.Printf("Unregistering replica: %s\n", r.Addr)
		delete(connectedReplicas, conn)
		conn.Close() // optionally close
	}
}

func broadcastToReplicas(resp RESPValue) {
	data, err := resp.Serialize()
	if err != nil {
		log.Println("Failed to serialize command:", err)
		return
	}

	replicaMu.RLock()
	defer replicaMu.RUnlock()

	for conn := range connectedReplicas {
		if _, err := conn.Write(data); err != nil {
			log.Printf("Replica write failed: %v — removing", err)
			replicaMu.RUnlock()
			unregisterReplica(conn)
			replicaMu.RLock()
		}
	}
}
func sendAckToReplica(conn net.Conn) (int64, error) {
	log.Println("sending REPLCONF GETACK * to replica")
	_, err := conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\ngetack\r\n$1\r\n*\r\n"))
	if err != nil {
		log.Printf("Failed to send REPLCONF GETACK *: %v", err)
		return 0, err
	}

	// Wait for ACK response
	reader := NewTrackingBufReader(conn)
	val, err := parseRESPValue(reader)
	if err != nil {
		log.Printf("Failed to read ACK from replica: %v", err)
		return 0, err
	}

	// Expecting something like: ["REPLCONF", "ACK", "12345"]
	if val.Type != Array || len(val.Array) != 3 {
		return 0, fmt.Errorf("unexpected ACK response format: %v", val)
	}

	cmd := strings.ToUpper(val.Array[0].String)
	subCmd := strings.ToUpper(val.Array[1].String)
	offsetStr := val.Array[2].String

	if cmd != "REPLCONF" || subCmd != "ACK" {
		return 0, fmt.Errorf("unexpected response: %s %s", cmd, subCmd)
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid offset in ACK: %v", err)
	}

	log.Printf("Replica acknowledged offset: %d", offset)
	return offset, nil
}

type Replica struct {
	Conn net.Conn
	Addr string
}
