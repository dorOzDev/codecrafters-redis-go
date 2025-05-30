package main

import (
	"log"
	"net"
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
			log.Println("ping replica to monitor connection")
			_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
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

type Replica struct {
	Conn net.Conn
	Addr string
}
