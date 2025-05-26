package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

var flagCache = make(map[string]string)

func GetFlagValue(flagName string) (string, bool) {
	if !strings.HasPrefix(flagName, "--") {
		flagName = "--" + flagName
	}

	// Check cache first
	if val, ok := flagCache[flagName]; ok {
		fmt.Printf("flag [%s] = %s (cached)\n", flagName, val)
		return val, true
	}

	// Search os.Args
	args := os.Args
	for i, arg := range args {
		if arg == flagName && i+1 < len(args) {
			val := args[i+1]
			flagCache[flagName] = val
			fmt.Printf("flag [%s] = %s (parsed and cached)\n", flagName, val)
			return val, true
		}
	}

	fmt.Printf("flag [%s] not found\n", flagName)
	return "", false
}

const (
	FlagDir        = "--dir"
	FlagDbFilename = "--dbfilename"
	FlagPort       = "--port"
	FlagReplicaof  = "--replicaof"
)

const PORT_DEFUALT = "6379"

func sendPing(conn net.Conn) error {
	_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	return err
}

func sendReplConf(conn net.Conn, key, value string) error {
	cmd := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(value), value)
	_, err := conn.Write([]byte(cmd))
	return err
}

func sendPsync(conn net.Conn) error {
	// This is a minimal PSYNC - full sync for now
	psyncCmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$1\r\n-1\r\n"
	_, err := conn.Write([]byte(psyncCmd))
	return err
}
