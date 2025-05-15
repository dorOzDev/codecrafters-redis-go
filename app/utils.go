package main

import (
	"fmt"
	"os"
	"strings"
)

type CaseInsensitiveMap[T any] struct {
	data map[string]T
}

func NewCaseInsensitiveMap[T any]() *CaseInsensitiveMap[T] {
	return &CaseInsensitiveMap[T]{data: make(map[string]T)}
}

func (m *CaseInsensitiveMap[T]) Set(key string, value T) {
	m.data[strings.ToLower(key)] = value
}

func (m *CaseInsensitiveMap[T]) Get(key string) (T, bool) {
	value, exists := m.data[strings.ToLower(key)]
	return value, exists
}

func (m *CaseInsensitiveMap[T]) Exists(key string) bool {
	_, exists := m.data[strings.ToLower(key)]
	return exists
}

func GetFlagValue(flagName string) (string, bool) {
	fmt.Println("attempt to get flag: ", flagName)
	if !strings.HasPrefix(flagName, "--") {
		flagName = "--" + flagName
	}
	args := os.Args
	for i, arg := range args {
		if arg == flagName && i+1 < len(args) {
			return args[i+1], true
		}
	}
	fmt.Println("did not find any value for flag: ", flagName)
	return "", false
}
