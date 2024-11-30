package main

import (
	"fmt"
	"log"
	"net"
)

const (
	REDIS_HOST = "0.0.0.0"
	REDIS_PORT = 6379
)

func handleConnection(c net.Conn) {
	defer c.Close()
	log.Printf("Accepted connection from %s", c.RemoteAddr())

	_, err := c.Write([]byte("+PONG\r\n"))

	if err != nil {
		logAndThrowError(err, "Error while writing response\n")
	}
}

func main() {
	listenAddress := fmt.Sprintf("%s:%d", REDIS_HOST, REDIS_PORT)
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logAndThrowError(err, fmt.Sprintf("Failed to bind to port: %d", REDIS_PORT))
	}

	log.Printf("Listening on port: %d", REDIS_PORT)

	for {
		c, err := l.Accept()
		if err != nil {
			logAndThrowError(err, "Error accepting connection")
		}

		go handleConnection(c)
	}

}

func logAndThrowError(err error, errorMessage string) {
	log.Fatalf("%s: %v", errorMessage, err)
}
