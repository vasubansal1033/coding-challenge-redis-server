package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	REDIS_HOST       = "0.0.0.0"
	REDIS_PORT       = 6379
	REDIS_CLI_PROMPT = "$ redis-cli > "
)

var handlers = map[string]func(Command) []byte{
	"PING": handlePing,
	"ECHO": handleEcho,
	"GET":  handleGet,
	"SET":  handleSet,
	"INFO": handleInfo,
}

var kvStore map[string]string = make(map[string]string)

func handleConnection(c net.Conn) {
	defer c.Close()

	for {
		// _, err := c.Write([]byte(REDIS_CLI_PROMPT))
		// if err != nil {
		// 	log.Printf("Error while writing to client: %s", c.RemoteAddr())
		// 	return
		// }

		buf := make([]byte, 1024)
		_, err := c.Read(buf)
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

		fmt.Printf("Got data: \n%s", buf)
		command := ReadNextCommand(buf)
		if command == nil {
			fmt.Println("Error reading command")
			c.Write([]byte("+PONG\r\n"))
			continue
		}

		commandHandler := handlers[strings.ToUpper(command.name)]
		if commandHandler == nil {
			fmt.Printf("unknown command %s. Skipping\n", command.name)
			c.Write([]byte("+PONG\r\n"))
			continue
		}
		response := commandHandler(*command)
		c.Write(response)
	}

}

func main() {

	redisPort := flag.Int("port", REDIS_PORT, "port on which redis server will run")
	flag.Parse()

	listenAddress := fmt.Sprintf("%s:%d", REDIS_HOST, *redisPort)
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logAndThrowError(err, fmt.Sprintf("Failed to bind to port: %d", *redisPort))
	}

	log.Printf("Listening on port: %d", *redisPort)

	defer l.Close()
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

func handlePing(cmd Command) []byte {
	fmt.Println("handle ping")
	return []byte("+PONG\r\n")
}

func handleEcho(cmd Command) []byte {
	fmt.Println("handle echo")
	return ToBulkString(cmd.args[0])
}

func handleSet(cmd Command) []byte {
	fmt.Println("handle set")
	kvStore[cmd.args[0]] = cmd.args[1]

	if len(cmd.args) > 2 && strings.ToUpper(cmd.args[2]) == "PX" {
		if expiry, err := strconv.Atoi(cmd.args[3]); err == nil {
			go func(key string, duration int) {
				time.Sleep(time.Duration(duration) * time.Millisecond)
				delete(kvStore, key)
			}(cmd.args[0], expiry)
		}
	}

	return ToBulkString("OK")
}

func handleGet(cmd Command) []byte {
	fmt.Println("handle get")

	val, ok := kvStore[cmd.args[0]]
	if !ok {
		return []byte("$-1\r\n")
	}

	return ToBulkString(val)
}

func handleInfo(cmd Command) []byte {
	fmt.Println("handle info")

	if cmd.args[0] == "replication" {
		return ToBulkString("role:master")
	}

	return ToBulkString("unsupported argument")
}
