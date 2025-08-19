package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RedisReplicationManager handles master-slave replication
type RedisReplicationManager struct {
	config           *RedisServerConfig
	logger           Logger
	slaveConnections map[int]net.Conn // port -> connection
	mu               sync.RWMutex
	byteOffset       int
	commandHandler   CommandHandler // Add command handler for processing master commands
}

func (rm *RedisReplicationManager) GetConnectedSlaves() int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	return len(rm.slaveConnections)
}

// NewRedisReplicationManager creates a new replication manager
func NewRedisReplicationManager(config *RedisServerConfig, logger Logger) *RedisReplicationManager {
	return &RedisReplicationManager{
		config:           config,
		logger:           logger,
		slaveConnections: make(map[int]net.Conn),
		byteOffset:       0,
		commandHandler:   nil, // Will be set later
	}
}

// SetCommandHandler sets the command handler for processing master commands
func (rm *RedisReplicationManager) SetCommandHandler(handler CommandHandler) {
	rm.commandHandler = handler
}

// RegisterSlave registers a new slave connection
func (rm *RedisReplicationManager) RegisterSlave(port int, conn net.Conn) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.slaveConnections[port] = conn
	rm.logger.Info("Registered slave on port %d", port)
	return nil
}

// ReplicateCommand sends a command to all registered slaves
func (rm *RedisReplicationManager) ReplicateCommand(cmd Command) error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	commandArray := []string{cmd.name}
	commandArray = append(commandArray, cmd.args...)

	data := ToArray(commandArray)

	for port, conn := range rm.slaveConnections {
		if err := rm.sendToSlave(port, conn, data); err != nil {
			rm.logger.Error("Failed to replicate command to slave on port %d: %v", port, err)
			// Remove failed connection
			delete(rm.slaveConnections, port)
		}
	}

	return nil
}

// StartSlaveConnection initiates connection to master and performs handshake
func (rm *RedisReplicationManager) StartSlaveConnection() (net.Conn, error) {
	if rm.config.Role != "slave" {
		return nil, fmt.Errorf("not configured as slave")
	}

	address := net.JoinHostPort(rm.config.MasterHost, strconv.Itoa(rm.config.MasterPort))
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to master at %s: %v", address, err)
	}

	rm.logger.Info("Connected to master at %s", address)

	if err := rm.performHandshake(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("handshake failed: %v", err)
	}

	return conn, nil
}

// StartSlaveConnectionWithReader initiates connection to master and performs handshake, returning the buffered reader
func (rm *RedisReplicationManager) StartSlaveConnectionWithReader() (net.Conn, *bufio.Reader, error) {
	if rm.config.Role != "slave" {
		return nil, nil, fmt.Errorf("not configured as slave")
	}

	address := net.JoinHostPort(rm.config.MasterHost, strconv.Itoa(rm.config.MasterPort))
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't connect to master at %s: %v", address, err)
	}

	rm.logger.Info("Connected to master at %s", address)

	buffReader, err := rm.performHandshakeWithReader(conn)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("handshake failed: %v", err)
	}

	return conn, buffReader, nil
}

// HandleMasterHandshake performs the complete handshake sequence with master
func (rm *RedisReplicationManager) HandleMasterHandshake() error {
	conn, buffReader, err := rm.StartSlaveConnectionWithReader()
	if err != nil {
		return err
	}

	// Start listening for commands from master
	go rm.handleMasterCommandsWithReader(conn, buffReader)

	return nil
}

func (rm *RedisReplicationManager) performHandshake(conn net.Conn) error {
	buffReader := bufio.NewReader(conn)

	// Step 1: Send PING
	if err := rm.sendCommand(conn, []string{"PING"}); err != nil {
		return fmt.Errorf("failed to send PING: %v", err)
	}

	if _, err := rm.readResponse(buffReader); err != nil {
		return fmt.Errorf("failed to read PING response: %v", err)
	}

	// Step 2: Send REPLCONF listening-port
	portStr := strconv.Itoa(rm.config.Port)
	if err := rm.sendCommand(conn, []string{"REPLCONF", "listening-port", portStr}); err != nil {
		return fmt.Errorf("failed to send REPLCONF listening-port: %v", err)
	}

	if _, err := rm.readResponse(buffReader); err != nil {
		return fmt.Errorf("failed to read REPLCONF listening-port response: %v", err)
	}

	// Step 3: Send REPLCONF capa
	if err := rm.sendCommand(conn, []string{"REPLCONF", "capa", "psync2"}); err != nil {
		return fmt.Errorf("failed to send REPLCONF capa: %v", err)
	}

	if _, err := rm.readResponse(buffReader); err != nil {
		return fmt.Errorf("failed to read REPLCONF capa response: %v", err)
	}

	// Step 4: Send PSYNC
	if err := rm.sendCommand(conn, []string{"PSYNC", "?", "-1"}); err != nil {
		return fmt.Errorf("failed to send PSYNC: %v", err)
	}

	// Read FULLRESYNC response
	fullResyncResp, err := rm.readResponse(buffReader)
	if err != nil {
		return fmt.Errorf("failed to read PSYNC response: %v", err)
	}

	if err := rm.parseFullResyncResponse(fullResyncResp); err != nil {
		return fmt.Errorf("failed to parse FULLRESYNC response: %v", err)
	}

	// Read RDB file
	if err := rm.readRDBFile(buffReader); err != nil {
		return fmt.Errorf("failed to read RDB file: %v", err)
	}

	rm.logger.Info("Handshake completed successfully")
	return nil
}

// performHandshakeWithReader performs handshake and returns the buffered reader for continued reading
func (rm *RedisReplicationManager) performHandshakeWithReader(conn net.Conn) (*bufio.Reader, error) {
	buffReader := bufio.NewReader(conn)

	// Step 1: Send PING
	if err := rm.sendCommand(conn, []string{"PING"}); err != nil {
		return nil, fmt.Errorf("failed to send PING: %v", err)
	}

	if _, err := rm.readResponse(buffReader); err != nil {
		return nil, fmt.Errorf("failed to read PING response: %v", err)
	}

	// Step 2: Send REPLCONF listening-port
	portStr := strconv.Itoa(rm.config.Port)
	if err := rm.sendCommand(conn, []string{"REPLCONF", "listening-port", portStr}); err != nil {
		return nil, fmt.Errorf("failed to send REPLCONF listening-port: %v", err)
	}

	if _, err := rm.readResponse(buffReader); err != nil {
		return nil, fmt.Errorf("failed to read REPLCONF listening-port response: %v", err)
	}

	// Step 3: Send REPLCONF capa
	if err := rm.sendCommand(conn, []string{"REPLCONF", "capa", "psync2"}); err != nil {
		return nil, fmt.Errorf("failed to send REPLCONF capa: %v", err)
	}

	if _, err := rm.readResponse(buffReader); err != nil {
		return nil, fmt.Errorf("failed to read REPLCONF capa response: %v", err)
	}

	// Step 4: Send PSYNC
	if err := rm.sendCommand(conn, []string{"PSYNC", "?", "-1"}); err != nil {
		return nil, fmt.Errorf("failed to send PSYNC: %v", err)
	}

	// Read FULLRESYNC response
	fullResyncResp, err := rm.readResponse(buffReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read PSYNC response: %v", err)
	}

	if err := rm.parseFullResyncResponse(fullResyncResp); err != nil {
		return nil, fmt.Errorf("failed to parse FULLRESYNC response: %v", err)
	}

	// Read RDB file
	if err := rm.readRDBFile(buffReader); err != nil {
		return nil, fmt.Errorf("failed to read RDB file: %v", err)
	}

	rm.logger.Info("Handshake completed successfully")
	return buffReader, nil
}

func (rm *RedisReplicationManager) sendCommand(conn net.Conn, args []string) error {
	data := ToArray(args)
	_, err := conn.Write(data)
	return err
}

func (rm *RedisReplicationManager) readResponse(reader *bufio.Reader) (string, error) {
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(response), nil
}

func (rm *RedisReplicationManager) parseFullResyncResponse(response string) error {
	// Expected format: "+FULLRESYNC <replid> <offset>\r\n"
	if !strings.HasPrefix(response, "+FULLRESYNC") {
		return fmt.Errorf("unexpected PSYNC response: %s", response)
	}

	parts := strings.Split(strings.TrimPrefix(response, "+FULLRESYNC "), " ")
	if len(parts) != 2 {
		return fmt.Errorf("invalid FULLRESYNC format: %s", response)
	}

	rm.config.MasterReplicaID = parts[0]

	offset, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("invalid offset in FULLRESYNC: %s", parts[1])
	}
	rm.config.MasterReplicaOffset = offset

	return nil
}

func (rm *RedisReplicationManager) readRDBFile(reader *bufio.Reader) error {
	// Read the bulk string length line
	lengthLine, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	length := getLength(lengthLine)

	// Read the RDB content
	rdbContent := make([]byte, length)
	_, err = io.ReadFull(reader, rdbContent)
	if err != nil {
		return err
	}

	rm.logger.Info("Received RDB file of %d bytes", length)
	return nil
}

func (rm *RedisReplicationManager) handleMasterCommands(conn net.Conn) {
	defer conn.Close()

	buffReader := bufio.NewReader(conn)

	for {
		command, commandBytesProcessed := ReadCommandArrayFromBuffer(buffReader)

		if command == nil {
			rm.logger.Info("Master connection closed")
			break
		}

		rm.logger.Debug("Received command from master: %s", command.name)

		// Process the command if we have a command handler
		if rm.commandHandler != nil {
			rm.logger.Info("Processing command from master: %s %v", command.name, command.args)

			// Check if this is a command that needs to respond back to master
			needsResponse := rm.shouldRespondToMaster(*command)
			rm.logger.Info("Command %s needs response: %v", command.name, needsResponse)

			var connToUse net.Conn
			if needsResponse {
				// Use the real connection for commands that need responses (like REPLCONF GETACK)
				connToUse = conn
				rm.logger.Info("Using real connection for response")
			} else {
				// Use dummy connection for data commands that don't need responses
				connToUse = &DummyConn{}
				rm.logger.Info("Using dummy connection (no response)")
			}

			if err := rm.commandHandler.Handle(connToUse, *command); err != nil {
				rm.logger.Error("Failed to process command from master: %v", err)
			} else {
				rm.logger.Info("Successfully processed command from master: %s", command.name)
			}
		}

		// Update byte offset
		rm.SetByteOffset(rm.GetByteOffset() + commandBytesProcessed)
	}
}

// handleMasterCommandsWithReader handles commands using an existing buffered reader
func (rm *RedisReplicationManager) handleMasterCommandsWithReader(conn net.Conn, buffReader *bufio.Reader) {
	defer conn.Close()

	for {
		command, commandBytesProcessed := ReadCommandArrayFromBuffer(buffReader)

		if command == nil {
			rm.logger.Info("Master connection closed")
			break
		}

		rm.logger.Debug("Received command from master: %s", command.name)

		// Process the command if we have a command handler
		if rm.commandHandler != nil {
			rm.logger.Info("Processing command from master: %s %v", command.name, command.args)

			// Check if this is a command that needs to respond back to master
			needsResponse := rm.shouldRespondToMaster(*command)
			rm.logger.Info("Command %s needs response: %v", command.name, needsResponse)

			var connToUse net.Conn
			if needsResponse {
				// Use the real connection for commands that need responses (like REPLCONF GETACK)
				connToUse = conn
				rm.logger.Info("Using real connection for response")
			} else {
				// Use dummy connection for data commands that don't need responses
				connToUse = &DummyConn{}
				rm.logger.Info("Using dummy connection (no response)")
			}

			if err := rm.commandHandler.Handle(connToUse, *command); err != nil {
				rm.logger.Error("Failed to process command from master: %v", err)
			} else {
				rm.logger.Info("Successfully processed command from master: %s", command.name)
			}
		}

		// Update byte offset
		rm.SetByteOffset(rm.GetByteOffset() + commandBytesProcessed)
	}
}

// shouldRespondToMaster checks if a command from master should get a response
func (rm *RedisReplicationManager) shouldRespondToMaster(cmd Command) bool {
	commandName := strings.ToUpper(cmd.name)

	// REPLCONF GETACK should always respond
	if commandName == "REPLCONF" && len(cmd.args) > 0 && strings.ToUpper(cmd.args[0]) == "GETACK" {
		return true
	}

	// Add other commands that need responses here if needed

	return false
}

func (rm *RedisReplicationManager) sendToSlave(port int, conn net.Conn, data []byte) error {
	_, err := conn.Write(data)
	if err != nil {
		rm.logger.Error("Failed to send data to slave on port %d: %v", port, err)
		return err
	}
	return nil
}

// GetByteOffset returns the current byte offset for replication
func (rm *RedisReplicationManager) GetByteOffset() int {
	return rm.byteOffset
}

// SetByteOffset sets the byte offset for replication
func (rm *RedisReplicationManager) SetByteOffset(offset int) {
	rm.byteOffset = offset
}

// DummyConn implements net.Conn but discards all writes (for slave processing master commands)
type DummyConn struct{}

func (d *DummyConn) Read(b []byte) (n int, err error)   { return 0, io.EOF }
func (d *DummyConn) Write(b []byte) (n int, err error)  { return len(b), nil } // Discard writes
func (d *DummyConn) Close() error                       { return nil }
func (d *DummyConn) LocalAddr() net.Addr                { return nil }
func (d *DummyConn) RemoteAddr() net.Addr               { return nil }
func (d *DummyConn) SetDeadline(t time.Time) error      { return nil }
func (d *DummyConn) SetReadDeadline(t time.Time) error  { return nil }
func (d *DummyConn) SetWriteDeadline(t time.Time) error { return nil }
