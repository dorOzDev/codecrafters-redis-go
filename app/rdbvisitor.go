package main

import "log"

type RDBVisitor interface {
	OnHeader(version int)
	OnAuxField(key, value string)
	OnDBStart(dbIndex int)
	OnEntry(key, value string, ttlMillis *int64)
	OnResizeDB(dbResize int, expireSize int)
}

type RDBStoreVisitor struct {
	store Store
	db    int // optional: track current DB index if you support SELECTDB
}

func NewRDBStoreVisitor(store Store) *RDBStoreVisitor {
	return &RDBStoreVisitor{store: store}
}

func (visitor *RDBStoreVisitor) OnHeader(version int) {
	log.Printf("Parsed RDB version: %d\n", version)
}

func (visitor *RDBStoreVisitor) OnAuxField(k, val string) {
	log.Printf("AUX field: %s = %s\n", k, val)
}

func (visitor *RDBStoreVisitor) OnDBStart(index int) {
	visitor.db = index
	log.Printf("Switched to DB %d\n", index)
}

func (visitor *RDBStoreVisitor) OnResizeDB(dbResize int, expireSize int) {
	log.Printf("dbResize: %d, expireSize: %d\n", dbResize, expireSize)
}

func (visitor *RDBStoreVisitor) OnEntry(key, value string, ttlMillis *int64) {
	log.Printf("DB %d: key: %s, value: %s, ttl: %d\n", visitor.db, key, value, ttlMillis)
	entry := Entry{
		Val:      value,
		ExpireAt: ttlMillis,
	}

	visitor.store.Set(key, entry)
}
