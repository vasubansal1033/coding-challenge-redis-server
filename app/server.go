package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"
)

const (
	REDIS_HOST = "0.0.0.0"
	REDIS_PORT = 6379
)

func handleConnection(c net.Conn) {
	defer c.Close()

	for {
		_, err := c.Read(make([]byte, 128))
		if err != nil {
			if err == io.EOF {
				log.Printf("Client closed connection: %s", c.RemoteAddr())
				return
			}
			if isConnectionResetError(err) {
				log.Printf("Connection reset by client: %s", c.RemoteAddr())
				return
			}

			log.Printf("Error while reading from %s: %v", c.RemoteAddr(), err)
			return
		}

		_, err = c.Write([]byte("+PONG\r\n"))

		if err != nil {
			logAndThrowError(err, "Error while writing response\n")
		}
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

		log.Printf("Accepted connection from %s", c.RemoteAddr())
		go handleConnection(c)
	}

}

func logAndThrowError(err error, errorMessage string) {
	log.Fatalf("%s: %v", errorMessage, err)
}

func isConnectionResetError(err error) bool {
	// Check if the error is a network error
	if nErr, ok := err.(*net.OpError); ok {
		// Check for syscall errors in the underlying cause
		if errno, ok := nErr.Err.(*os.SyscallError).Err.(syscall.Errno); ok {
			return errno == syscall.ECONNRESET
		}
	}
	return false
}
