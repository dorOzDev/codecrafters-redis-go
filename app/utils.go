package main

import (
	"fmt"
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
