package main

import (
	"fmt"
	"time"
)

type RDBVisitor interface {
	OnHeader(version int)
	OnAuxField(key, value string)
	OnDBStart(dbIndex int)
	OnEntry(key, value string, ttlMillis int64)
}

type RDBStoreVisitor struct {
	store Store
	db    int // optional: track current DB index if you support SELECTDB
}

func NewRDBStoreVisitor(store Store) *RDBStoreVisitor {
	return &RDBStoreVisitor{store: store}
}

func (visitor *RDBStoreVisitor) OnHeader(version int) {
	fmt.Printf("Parsed RDB version: %d\n", version)
}

func (visitor *RDBStoreVisitor) OnAuxField(k, val string) {
	fmt.Printf("AUX field: %s = %s\n", k, val)
}

func (visitor *RDBStoreVisitor) OnDBStart(index int) {
	visitor.db = index
	fmt.Printf("Switched to DB %d\n", index)
}

func (visitor *RDBStoreVisitor) OnEntry(key, value string, ttlMillis int64) {
	now := time.Now()
	var createdAt time.Time

	if ttlMillis > 0 {
		createdAt = now.Add(-time.Duration(ttlMillis) * time.Millisecond)
	} else {
		createdAt = now
	}

	entry := Entry{
		Val:       value,
		TTL:       time.Duration(ttlMillis) * time.Millisecond,
		CreatedAt: createdAt,
	}

	visitor.store.Set(key, entry)
}
