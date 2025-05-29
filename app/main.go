package main

import (
	"log"
)

func main() {
	log.Println("Logs from your program will appear here!")
	connectionHandler, err := CreateConnectionHandler()
	if err != nil {
		log.Fatal("unable to start server: ", err)
	}
	connectionHandler.HandleConnection()
}
