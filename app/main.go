package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
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
		handler, exists := handlerMap.Get(text)
		if exists {
			scanner.Scan()
			value := scanner.Text()
			fmt.Printf("running handler %s, with value: %s", handler.handlerName(), value)
			conn.Write([]byte(handler.handlerFunc()(value)))
		}
	}
}

type CommandHandler interface {
	commandName() string
	handlerName() string
	handlerFunc() HandlerFunc
}

type PingCommandHandler struct{}

func (PingCommandHandler) handlerName() string {
	return "ping handler"
}

func (PingCommandHandler) commandName() string {
	return "PING"
}

func (PingCommandHandler) handlerFunc() HandlerFunc {
	return handlePong
}

type EchoCommandHandler struct{}

func (EchoCommandHandler) handlerName() string {
	return "echo handler"
}

func (EchoCommandHandler) commandName() string {
	return "ECHO"
}

func (EchoCommandHandler) handlerFunc() HandlerFunc {
	return handleEcho
}

type HandlerFunc func(str string) string

var handlerMap = NewCaseInsensitiveMap[CommandHandler]()

func init() {
	handlers := []CommandHandler{
		PingCommandHandler{},
		EchoCommandHandler{},
	}

	for _, handler := range handlers {
		handlerMap.Set(handler.commandName(), handler)
	}
}

func handlePong(str string) string {
	return "+PONG\r\n"
}

func handleEcho(str string) string {
	return fmt.Sprintf("+%s\r\n", str)
}
