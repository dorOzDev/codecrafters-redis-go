package main

import (
	"log"
	"strings"
	"sync"
)

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
			log.Println("unsupported info section", name)
		}

	}

	return filtered
}
