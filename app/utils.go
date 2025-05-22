package main

import (
	"fmt"
	"os"
	"strings"
)

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

const (
	FlagDir        = "--dir"
	FlagDbFilename = "--dbfilename"
	FlagPort       = "--port"
	FlagReplicaof  = "--replicaof"
)
