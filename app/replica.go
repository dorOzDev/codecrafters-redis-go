package main

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	connectedReplicas sync.Map
)

var totalBytes atomic.Int64

func registerReplica(conn net.Conn) {

	connectedReplicas.Store(conn, &ReplicaState{
		Conn: conn,
		Addr: conn.RemoteAddr().String(),
	})

	log.Printf("Registered replica: %s\n", conn.RemoteAddr().String())
	// TODO rethink of the monitor mechanism - temporary disable
	//monitorReplicaConnection(conn)
}

func monitorReplicaConnection(conn net.Conn) {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			err := sendAckToReplica(conn)
			if err != nil {
				log.Printf("Replica %v is unreachable (ping failed): %v", conn.RemoteAddr(), err)
				unregisterReplica(conn)
				return
			}
		}
	}()
}

func GetAllConnectedReplicas() []*ReplicaState {
	var replicasState []*ReplicaState

	connectedReplicas.Range(func(_ any, value any) bool {
		replica := value.(*ReplicaState)
		replicasState = append(replicasState, replica)
		return true
	})

	return replicasState
}

func unregisterReplica(conn net.Conn) {
	val, ok := connectedReplicas.Load(conn)
	if ok {
		replica := val.(*ReplicaState)
		log.Printf("Unregistering replica: %s\n", replica.Addr)
		connectedReplicas.Delete(conn)
		conn.Close() // optionally close
	}
}

func broadcastToReplicas(resp RESPValue) {
	data, err := resp.Serialize()
	if err != nil {
		log.Println("Failed to serialize command:", err)
		return
	}

	newOffset := totalBytes.Add(int64(len(data)))

	connectedReplicas.Range(func(key, value any) bool {
		conn := key.(net.Conn)
		state := value.(*ReplicaState)

		if _, err := conn.Write(data); err != nil {
			log.Printf("Replica write failed: %v — removing", err)
			unregisterReplica(conn)
			return true // continue with next replica
		}

		state.PendingOffset = newOffset
		return true
	})
}

func sendAckToReplica(conn net.Conn) error {
	log.Println("sending REPLCONF GETACK * to replica")
	_, err := conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
	if err != nil {
		log.Printf("Failed to send REPLCONF GETACK *: %v", err)
		return err
	}

	return nil
}

type ReplicaState struct {
	Conn           net.Conn
	Addr           string
	LastAckOffset  int64
	PendingOffset  int64
	LastAckRequest time.Time
	Mu             sync.Mutex
}

func (replicateState *ReplicaState) NeedsAck() bool {
	replicateState.Mu.Lock()
	defer replicateState.Mu.Unlock()
	return replicateState.LastAckOffset < replicateState.PendingOffset
}

func (replicaState *ReplicaState) SendAck(throttle time.Duration) error {
	replicaState.Mu.Lock()
	defer replicaState.Mu.Unlock()

	now := time.Now()
	if now.Sub(replicaState.LastAckRequest) < throttle {
		log.Printf("replica is throttled, skipping send GETACK")
		return nil
	}
	replicaState.LastAckRequest = now

	err := sendAckToReplica(replicaState.Conn)
	if err != nil {
		return err
	}

	return nil
}

func UpdateReplicaAckOffsetByConn(conn net.Conn, offset int64) {
	val, ok := connectedReplicas.Load(conn)
	if !ok {
		log.Printf("UpdateReplicaAckOffsetByConn: no ReplicaState found for connection %v", conn.RemoteAddr())
		return
	}

	rs := val.(*ReplicaState)
	rs.Mu.Lock()
	defer rs.Mu.Unlock()
	rs.LastAckOffset = offset
	log.Printf("Replica [%v] updated ACK offset to %d", conn.RemoteAddr(), offset)
}
