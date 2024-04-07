package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

const (
	REDIS_HOST = "0.0.0.0"
	REDIS_PORT = 6379
)

func main() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", REDIS_HOST, REDIS_PORT))
	if err != nil {
		logAndThrowError(err, fmt.Sprintf("Failed to bind to port: %d", REDIS_PORT))
	}

	log.Printf("Listening on port: %d", REDIS_PORT)

	_, err = l.Accept()
	if err != nil {
		logAndThrowError(err, "Error accepting connection")
	}
	
	log.Printf("Accepted connection. Now listening on port: %d", REDIS_PORT)
}

func logAndThrowError(err error, errorMessage string) {
	log.Fatalf("%s: %v", errorMessage, err)
	os.Exit(1)
}