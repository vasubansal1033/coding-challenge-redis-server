package main

import (
	"bufio"
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

var isWrite = map[string]bool{
	"SET": true,
}

var slaveNodePortConnectionMap map[int]net.Conn = make(map[int]net.Conn)

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

		if isWrite[command.name] {
			for slavePort, slaveConnection := range slaveNodePortConnectionMap {
				replicateToSlaveNode(slavePort, *command, slaveConnection)
			}
		}
	}
}

func main() {

	redisPort := flag.Int("port", REDIS_PORT, "port on which redis server will run")
	masterAddress := flag.String("replicaof", "", "address and port of master replica")
	flag.Parse()

	serverConfig.ListeningPort = *redisPort

	listenAddress := fmt.Sprintf("%s:%d", REDIS_HOST, *redisPort)
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logAndThrowError(err, fmt.Sprintf("Failed to bind to port: %d", *redisPort))
	}

	if *masterAddress != "" {
		serverConfig.Role = "slave"

		masterAddr := strings.Split(*masterAddress, " ")
		serverConfig.MasterHost = masterAddr[0]

		masterPort, err := strconv.Atoi(masterAddr[1])
		if err != nil {
			logAndThrowError(err, fmt.Sprintf("Invalid master address %s", *masterAddress))
		}

		serverConfig.MasterPort = masterPort
		go sendHandshakeToMaster()
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
	response := []byte("+PONG\r\n")

	conn.Write(response)
}

func handleEcho(conn net.Conn, cmd Command) {
	response := ToBulkString(cmd.args[0])

	conn.Write(response)
}

func handleSet(conn net.Conn, cmd Command) {
	kvStore[cmd.args[0]] = cmd.args[1]

	if len(cmd.args) > 2 && strings.ToUpper(cmd.args[2]) == "PX" {
		if expiry, err := strconv.Atoi(cmd.args[3]); err == nil {
			go func(key string, duration int) {
				time.Sleep(time.Duration(duration) * time.Millisecond)
				delete(kvStore, key)
			}(cmd.args[0], expiry)
		}
	}

	// Parse the remote address into a *net.TCPAddr
	remoteAddr := conn.RemoteAddr().String()
	remoteTCPAddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
	if err != nil {
		log.Fatalf(err.Error())
	}

	targetTCPAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%v:%v", serverConfig.MasterHost, serverConfig.MasterPort))
	if err != nil {
		log.Fatalf(err.Error())
	}

	fromMaster := remoteTCPAddr.Port == targetTCPAddr.Port

	if !fromMaster {
		response := ToSimpleString("OK")
		conn.Write(response)
	}
}

func handleGet(conn net.Conn, cmd Command) {
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

	response := []byte{}
	switch cmd.args[0] {
	case "listening-port":
		port, err := strconv.Atoi(cmd.args[1])
		if err != nil {
			log.Printf("invalid port in replconf: %v", cmd.args[1])
		} else {
			slaveNodePortConnectionMap[port] = conn
		}
		response = ToSimpleString("OK")

	case "GETACK":
		offset := 0
		response = ToArray([]string{"REPLCONF", "ACK", strconv.Itoa(offset)})

	case "capa":
		response = ToSimpleString("OK")
	}

	conn.Write(response)
}

func handlePsync(conn net.Conn, cmd Command) {
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

func sendHandshakeToMaster() net.Conn {
	address := fmt.Sprintf("%s:%d", serverConfig.MasterHost, serverConfig.MasterPort)
	m, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatalln("couldn't connect to master at ", address)
	}

	buffReader := bufio.NewReader(m)

	// send ping
	m.Write(ToArray([]string{"PING"}))

	resp, err := buffReader.ReadString('\n')
	if err != nil {
		log.Fatalf("couldn't read response from master replica")
	}

	// sending two replconf
	m.Write(ToArray([]string{"replconf", "listening-port", strconv.Itoa(serverConfig.ListeningPort)}))

	resp, err = buffReader.ReadString('\n')
	if err != nil {
		log.Fatalf("couldn't read response from master replica")
	}

	m.Write(ToArray([]string{"replconf", "capa", "psync2"}))

	resp, err = buffReader.ReadString('\n')
	if err != nil {
		log.Fatalf("couldn't read response from master replica")
	}

	// send psync
	m.Write(ToArray([]string{"psync", "?", "-1"}))

	resp, err = buffReader.ReadString('\n')
	if err != nil {
		log.Fatalf("couldn't read response from master replica")
	}

	_, fullResyncSimpleStringResp := ReadNextRESP([]byte(resp))
	fullResyncSimpleStringArgs := strings.Split(string(fullResyncSimpleStringResp.Data), " ")

	serverConfig.MasterReplicaId = fullResyncSimpleStringArgs[1]
	serverConfig.MasterReplicaOffset, err = strconv.Atoi(fullResyncSimpleStringArgs[2])
	if err != nil {
		log.Fatalf("Invalid offset sent in fullresync by master")
	}

	resp, err = buffReader.ReadString('\n')
	if err != nil {
		log.Fatalf("couldn't read response from master replica")
	}

	lengthOfRdb := getLength(resp)

	rdbContent := make([]byte, lengthOfRdb)
	io.ReadFull(buffReader, rdbContent)

	// consume pipelined set commands
	for {
		command := ReadCommandArrayFromBuffer(buffReader)
		if command == nil {
			break
		}

		commandHandler := handlers[strings.ToUpper(command.name)]
		commandHandler(m, *command)
	}

	return m
}

func replicateToSlaveNode(slavePort int, cmd Command, conn net.Conn) {
	commandArray := []string{}
	commandArray = append(commandArray, cmd.name)
	for _, command := range cmd.args {
		commandArray = append(commandArray, command)
	}

	conn.Write(ToArray(commandArray))
}
