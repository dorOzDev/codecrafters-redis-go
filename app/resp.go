package main

type RESPType byte

const (
	SimpleString RESPType = '+'
	Error        RESPType = '-'
	Integer      RESPType = ':'
	BulkString   RESPType = '$'
	Array        RESPType = '*'
)

type RESPValue struct {
	Type    RESPType
	String  string
	Integer int64
	Array   []RESPValue
	IsNil   bool
}
