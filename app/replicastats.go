package main

import "sync/atomic"

type ReplicaStats struct {
	BytesRead uint64
}

func (r *ReplicaStats) writeBytes(n int) {
	atomic.AddUint64(&r.BytesRead, uint64(n))
}

func (r *ReplicaStats) getBytesRead() uint64 {
	return atomic.LoadUint64(&r.BytesRead)
}
