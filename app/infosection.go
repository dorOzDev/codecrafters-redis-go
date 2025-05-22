package main

import (
	"bytes"
	"fmt"
	"strings"
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
			fmt.Println("in replica: ", val)
			role = "slave"
		}
	})

	return role
}

type InfoSection struct {
	Name    string
	GetInfo func() string
}

const (
	InfoSectionReplication = "replication"
)

var supportedInfoSections = []InfoSection{
	{
		Name:    InfoSectionReplication,
		GetInfo: replicationInfo,
	},
}

var (
	sectionMap     map[string]InfoSection
	sectionMapOnce sync.Once
)

/** return all supported InfoSection map*/
func getSectionMap() map[string]InfoSection {
	sectionMapOnce.Do(func() {
		m := make(map[string]InfoSection, len(supportedInfoSections))
		for _, section := range supportedInfoSections {
			m[section.Name] = section
		}

		sectionMap = m
	})

	return sectionMap
}

/** return a map of all the supported info section. if no sections was provided then return the whole map*/
func getSectionsByNames(names ...string) map[string]InfoSection {
	allSections := getSectionMap()
	if len(names) == 0 {
		return allSections
	}

	filtered := make(map[string]InfoSection, len(names))

	for _, name := range names {
		lowerCase := strings.ToLower(name)
		section, isExist := allSections[lowerCase]
		if isExist {
			filtered[name] = section
		} else {
			fmt.Println("unsupported info section", name)
		}

	}

	return filtered
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
		MasterReplid:     getMasterReplid(),
		MasterReplOffset: getMasterReplOffset()}

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

func getMasterReplid() string {
	return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

func getMasterReplOffset() int64 {
	return 0
}
