package main

import (
	"bytes"
	"log"
	"sync"
	"text/template"
)

var (
	role     string = "master"
	initRole sync.Once
)

func getRole() string {
	initRole.Do(func() {
		val, exists := GetFlagValue(FlagReplicaof)
		if exists {
			log.Println("in replica: ", val)
			role = "slave"
		}
	})

	return role
}

const replicationTemplate = `# Replication
role:{{.Role}}
master_replid:{{.MasterReplid}}
master_repl_offset:{{.MasterReplOffset}}`

type ReplicationData struct {
	Role             string
	MasterReplid     string
	MasterReplOffset int64
}

func replicationInfo() string {
	data := ReplicationData{
		Role:             getRole(),
		MasterReplid:     GetMasterReplId(),
		MasterReplOffset: GetMasterReplOffset()}

	tmpl, err := template.New(InfoSectionReplication).Parse(replicationTemplate)
	if err != nil {
		panic(err) // or log if you prefer not to crash
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		panic(err)
	}

	return buf.String()
}

func GetMasterReplId() string {
	return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

func GetMasterReplOffset() int64 {
	return 0
}
