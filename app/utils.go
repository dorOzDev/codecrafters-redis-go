package main

import "strings"

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
