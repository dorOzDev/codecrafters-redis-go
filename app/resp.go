package main

import (
	"fmt"
	"strings"
)

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
}

// instead of func(RESPValue, io.Writer) error
type RESPSerializer func(v RESPValue) ([]byte, error)

var serializers = make(map[RESPType]RESPSerializer)

func init() {
	serializers[SimpleString] = serializeSimpleString
	serializers[Error] = serializeError
	serializers[Integer] = serializeInteger
	serializers[BulkString] = serializeBulkString
	serializers[Array] = serializeArray
}

func (v RESPValue) Serialize() ([]byte, error) {
	serializer, ok := serializers[v.Type]
	if !ok {
		return nil, fmt.Errorf("no serializer for RESPType %q", v.Type)
	}

	return serializer(v)
}

func serializeSimpleString(v RESPValue) ([]byte, error) {
	// “+OK\r\n”
	s := fmt.Sprintf("%c%s\r\n", v.Type, v.String)
	return []byte(s), nil
}

func serializeError(v RESPValue) ([]byte, error) {
	s := fmt.Sprintf("%c%s\r\n", v.Type, v.String)
	return []byte(s), nil
}

func serializeInteger(v RESPValue) ([]byte, error) {
	s := fmt.Sprintf("%c%d\r\n", v.Type, v.Integer)
	return []byte(s), nil
}

func serializeBulkString(v RESPValue) ([]byte, error) {
	if v.String == "" {
		return []byte(fmt.Sprintf("%c-1\r\n", v.Type)), nil
	}
	s := fmt.Sprintf("%c%d\r\n%s\r\n", v.Type, len(v.String), v.String)
	return []byte(s), nil
}

func serializeArray(v RESPValue) ([]byte, error) {
	if v.Array == nil {
		return []byte(fmt.Sprintf("%c-1\r\n", v.Type)), nil
	}
	// start with the array header
	buf := &strings.Builder{}
	buf.WriteString(fmt.Sprintf("%c%d\r\n", v.Type, len(v.Array)))
	// append each element’s serialized bytes
	for _, elem := range v.Array {
		b, err := elem.Serialize() // recursive
		if err != nil {
			return nil, err
		}
		buf.Write(b)
	}
	return []byte(buf.String()), nil
}

func (v RESPValue) ToString() (string, error) {
	byteArr, err := v.Serialize()
	if err != nil {
		return "", err
	}

	return string(byteArr), err
}
