package main

import "sync/atomic"

type ReplicaTrackingBytes struct {
	BytesRead uint64
}

func (r *ReplicaTrackingBytes) writeBytes(n int) {
	atomic.AddUint64(&r.BytesRead, uint64(n))
}
