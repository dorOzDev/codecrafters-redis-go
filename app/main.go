package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println(text)
		conn.Write([]byte(handleCommand(text)))
	}
}

func handleCommand(text string) string {
	cleanString := strings.TrimSpace(text)
	commandHandler, exsits := handlerMap.Get(cleanString)
	if exsits {
		fmt.Print("running function ", commandHandler.handlerName())
		return commandHandler.handlerFunc()(cleanString)
	}

	return ""
}

type CommandHandler interface {
	handlerName() string
	handlerFunc() HandlerFunc
}

type PingCommandHandler struct{}

func (PingCommandHandler) handlerName() string {
	return "ping handler"
}

func (PingCommandHandler) handlerFunc() HandlerFunc {
	return handlePong
}

type EchoCommandHandler struct{}

func (EchoCommandHandler) handlerName() string {
	return "echo handler"
}

func (EchoCommandHandler) handlerFunc() HandlerFunc {
	return handleEcho
}

type HandlerFunc func(str string) string

var handlerMap = NewCaseInsensitiveMap[CommandHandler]()

func init() {
	handlerMap.Set("PONG", PingCommandHandler{})
	handlerMap.Set("ECHO", EchoCommandHandler{})
}

func handlePong(str string) string {
	return "+PONG\r\n"
}

func handleEcho(str string) string {
	return fmt.Sprintf("+%s\r\n", str)
}
