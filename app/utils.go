package main

import (
	"fmt"
	"log"
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
		log.Printf("flag [%s] = %s (cached)\n", flagName, val)
		return val, true
	}

	// Search os.Args
	args := os.Args
	for i, arg := range args {
		if arg == flagName && i+1 < len(args) {
			val := args[i+1]
			flagCache[flagName] = val
			log.Printf("flag [%s] = %s (parsed and cached)\n", flagName, val)
			return val, true
		}
	}

	log.Printf("flag [%s] not found\n", flagName)
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
	cmd := "*1\r\n$4\r\nPING\r\n"
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return err
	}
	resp, err := readLine(conn)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "+PONG") {
		return fmt.Errorf("expected +PONG, got %q", resp)
	}
	return nil
}

func sendReplConf(conn net.Conn, key, value string) error {
	cmd := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(value), value)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return err
	}
	resp, err := readLine(conn)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("expected +OK, got %q", resp)
	}
	return nil
}

func sendPsync(conn net.Conn) error {
	psyncCmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	if _, err := conn.Write([]byte(psyncCmd)); err != nil {
		return err
	}
	resp, err := readLine(conn)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "+FULLRESYNC") {
		return fmt.Errorf("unexpected PSYNC response: %q", resp)
	}
	return nil
}

func readLine(conn net.Conn) (string, error) {
	buf := make([]byte, 0, 512)
	tmp := make([]byte, 1)
	for {
		_, err := conn.Read(tmp)
		if err != nil {
			return "", err
		}
		if tmp[0] == '\n' && len(buf) > 0 && buf[len(buf)-1] == '\r' {
			return string(buf[:len(buf)-1]), nil // strip \r
		}
		buf = append(buf, tmp[0])
	}
}
