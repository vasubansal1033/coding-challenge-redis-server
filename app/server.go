package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
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
	ALPHANUMERIC_SET = "abcdefghijklmnopqrstuvwxyz0123456789"
	EMPTY_RDB_HEX    = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

var handlers = map[string]func(net.Conn, Command){
	"PING":     handlePing,
	"ECHO":     handleEcho,
	"GET":      handleGet,
	"SET":      handleSet,
	"INFO":     handleInfo,
	"REPLCONF": handleReplConf,
	"PSYNC":    handlePsync,
}

type ServerConfig struct {
	Role                string `json:"role"`
	MasterReplicaId     string `json:"masterReplicaId"`
	MasterReplicaOffset int    `json:"masterReplicaOffset"`
	MasterHost          string `json:"masterHost"`
	MasterPort          int    `json:"masterPort"`
	ListeningPort       int    `json:"listeningPort"`
}

var serverConfig = ServerConfig{
	Role:                "master",
	MasterReplicaId:     generateRandomString(40),
	MasterReplicaOffset: 0,
}

var kvStore map[string]string = make(map[string]string)

func handleConnection(c net.Conn) {
	defer c.Close()

	for {
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

		commandHandler(c, *command)
	}

}

func main() {

	redisPort := flag.Int("port", REDIS_PORT, "port on which redis server will run")
	masterAddress := flag.String("replicaof", "", "address and port of master replica")
	flag.Parse()

	serverConfig.ListeningPort = *redisPort

	if *masterAddress != "" {
		serverConfig.Role = "slave"

		masterAddr := strings.Split(*masterAddress, " ")
		serverConfig.MasterHost = masterAddr[0]

		masterPort, err := strconv.Atoi(masterAddr[1])
		if err != nil {
			logAndThrowError(err, fmt.Sprintf("Invalid master address %s", *masterAddress))
		}

		serverConfig.MasterPort = masterPort
		sendHandshakeToMaster()
	}

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

func handlePing(conn net.Conn, cmd Command) {
	fmt.Println("handle ping")
	response := []byte("+PONG\r\n")

	conn.Write(response)
}

func handleEcho(conn net.Conn, cmd Command) {
	fmt.Println("handle echo")
	response := ToBulkString(cmd.args[0])

	conn.Write(response)
}

func handleSet(conn net.Conn, cmd Command) {
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

	response := ToSimpleString("OK")
	conn.Write(response)
}

func handleGet(conn net.Conn, cmd Command) {
	fmt.Println("handle get")

	val, ok := kvStore[cmd.args[0]]

	var response []byte
	if !ok {
		response = []byte("$-1\r\n")
	} else {
		response = ToBulkString(val)
	}

	conn.Write(response)
}

func handleInfo(conn net.Conn, cmd Command) {
	fmt.Println("handle info")

	var response []byte
	if cmd.args[0] == "replication" {
		infoOutput := fmt.Sprintf("role:%s", serverConfig.Role)
		infoOutput += fmt.Sprintf(":master_replid:%s", serverConfig.MasterReplicaId)
		infoOutput += fmt.Sprintf(":master_repl_offset:%d", serverConfig.MasterReplicaOffset)
		response = ToBulkString(infoOutput)
	} else {
		response = ToBulkString("unsupported argument")
	}

	conn.Write(response)
}

func handleReplConf(conn net.Conn, cmd Command) {
	fmt.Println("handle replconf")

	response := ToSimpleString("OK")
	conn.Write(response)
}

func handlePsync(conn net.Conn, cmd Command) {
	fmt.Println("handle psync")

	response := []byte(ToSimpleString(fmt.Sprintf("FULLRESYNC %s %d", serverConfig.MasterReplicaId, serverConfig.MasterReplicaOffset)))
	conn.Write(response)

	decodedHex, err := hex.DecodeString(EMPTY_RDB_HEX)
	if err == nil {
		response := append([]byte(fmt.Sprintf("$%d\r\n", len(decodedHex))), decodedHex...)
		conn.Write(response)
	}
}

func generateRandomString(length int) string {
	result := make([]byte, length)

	for i := 0; i < length; i++ {
		result[i] = ALPHANUMERIC_SET[rand.Intn(len(ALPHANUMERIC_SET))]
	}

	return string(result)
}

func sendHandshakeToMaster() {
	address := fmt.Sprintf("%s:%d", serverConfig.MasterHost, serverConfig.MasterPort)
	m, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatalln("couldn't connect to master at ", address)
	}

	buf := make([]byte, 1024)

	// send ping
	m.Write(ToArray([]string{"PING"}))
	_, err = m.Read(buf)
	if err != nil {
		log.Fatalf("couldn't read response from master replica")
	}

	log.Println(string(buf))

	// sending two replconf
	m.Write(ToArray([]string{"replconf", "listening-port", strconv.Itoa(serverConfig.ListeningPort)}))
	_, err = m.Read(buf)
	if err != nil {
		log.Fatalf("couldn't read response from master replica")
	}

	log.Println(string(buf))

	m.Write(ToArray([]string{"replconf", "capa", "psync2"}))
	_, err = m.Read(buf)
	if err != nil {
		log.Fatalf("couldn't read response from master replica")
	}

	log.Println(string(buf))

	// send psync
	m.Write(ToArray([]string{"psync", "?", "-1"}))
	_, err = m.Read(buf)
	if err != nil {
		log.Fatalf("couldn't read response from master replica")
	}

	log.Println(string(buf))
}
