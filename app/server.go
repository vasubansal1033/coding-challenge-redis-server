package main

import (
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
)

var handlers = map[string]func(Command) []byte{
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
		response := commandHandler(*command)
		c.Write(response)
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

	return ToSimpleString("OK")
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
		infoOutput := fmt.Sprintf("role:%s", serverConfig.Role)
		infoOutput += fmt.Sprintf(":master_replid:%s", serverConfig.MasterReplicaId)
		infoOutput += fmt.Sprintf(":master_repl_offset:%d", serverConfig.MasterReplicaOffset)
		return ToBulkString(infoOutput)
	}

	return ToBulkString("unsupported argument")
}

func handleReplConf(cmd Command) []byte {
	fmt.Println("handle replconf")

	return ToSimpleString("OK")
}

func handlePsync(cmd Command) []byte {
	fmt.Println("handle psync")

	return []byte(ToSimpleString(fmt.Sprintf("FULLRESYNC %s %d", serverConfig.MasterReplicaId, serverConfig.MasterReplicaOffset)))
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
