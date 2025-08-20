package main

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// RedisCommandHandler implements the CommandHandler interface
type RedisCommandHandler struct {
	kvStore            KVStore
	config             *RedisServerConfig
	replicationManager ReplicationManager
	logger             Logger
	commandHistory     *CommandHistory
}

// NewRedisCommandHandler creates a new command handler
func NewRedisCommandHandler(kvStore KVStore, config *RedisServerConfig, replManager ReplicationManager, logger Logger, commandHistory *CommandHistory) *RedisCommandHandler {
	return &RedisCommandHandler{
		kvStore:            kvStore,
		config:             config,
		replicationManager: replManager,
		logger:             logger,
		commandHistory:     commandHistory,
	}
}

// writeCommands defines which commands modify data
var writeCommands = map[string]bool{
	"SET": true,
}

// Handle processes a Redis command
func (h *RedisCommandHandler) Handle(conn net.Conn, cmd Command) error {
	commandName := strings.ToUpper(cmd.name)
	defer h.commandHistory.AddCommand(cmd)

	switch commandName {
	case "PING":
		return h.handlePing(conn, cmd)
	case "ECHO":
		return h.handleEcho(conn, cmd)
	case "GET":
		return h.handleGet(conn, cmd)
	case "SET":
		return h.handleSet(conn, cmd)
	case "INFO":
		return h.handleInfo(conn, cmd)
	case "REPLCONF":
		return h.handleReplConf(conn, cmd)
	case "PSYNC":
		return h.handlePsync(conn, cmd)
	case "WAIT":
		return h.handleWait(conn, cmd)
	case "CONFIG":
		return h.handleConfig(conn, cmd)
	default:
		h.logger.Error("Unknown command: %s", commandName)
		return h.writeResponse(conn, ToSimpleString("PONG"))
	}
}

// CanHandle checks if this handler can process the given command
func (h *RedisCommandHandler) CanHandle(commandName string) bool {
	commandName = strings.ToUpper(commandName)
	supportedCommands := []string{"PING", "ECHO", "GET", "SET", "INFO", "REPLCONF", "PSYNC", "WAIT", "CONFIG"}

	for _, cmd := range supportedCommands {
		if cmd == commandName {
			return true
		}
	}
	return false
}

// IsWriteCommand checks if the command modifies data
func (h *RedisCommandHandler) IsWriteCommand(commandName string) bool {
	return writeCommands[strings.ToUpper(commandName)]
}

func (h *RedisCommandHandler) handlePing(conn net.Conn, cmd Command) error {
	h.logger.Info("Received PING from %s", conn.RemoteAddr())
	// For slave connections, we don't respond to PING from master
	if h.config.Role == "slave" {
		return nil
	}
	return h.writeResponse(conn, ToSimpleString("PONG"))
}

func (h *RedisCommandHandler) handleEcho(conn net.Conn, cmd Command) error {
	if len(cmd.args) == 0 {
		return h.writeResponse(conn, ToSimpleString("ERR wrong number of arguments"))
	}
	return h.writeResponse(conn, ToBulkString(cmd.args[0]))
}

func (h *RedisCommandHandler) handleSet(conn net.Conn, cmd Command) error {
	if len(cmd.args) < 2 {
		return h.writeResponse(conn, ToSimpleString("ERR wrong number of arguments"))
	}

	key := cmd.args[0]
	value := cmd.args[1]
	var expiry time.Duration

	// Parse expiration if provided
	if len(cmd.args) > 2 && strings.ToUpper(cmd.args[2]) == "PX" {
		if len(cmd.args) < 4 {
			return h.writeResponse(conn, ToSimpleString("ERR wrong number of arguments"))
		}

		expiryMs, err := strconv.Atoi(cmd.args[3])
		if err != nil {
			return h.writeResponse(conn, ToSimpleString("ERR invalid expire time"))
		}
		expiry = time.Duration(expiryMs) * time.Millisecond
	}

	// Store the key-value pair
	if err := h.kvStore.Set(key, value, expiry); err != nil {
		h.logger.Error("Failed to set key %s: %v", key, err)
		return h.writeResponse(conn, ToSimpleString("ERR internal error"))
	}

	// Check if this is from master (for slave nodes)
	if h.config.Role == "slave" && h.isFromMaster(conn) {
		return nil // Don't respond to master
	}

	return h.writeResponse(conn, ToSimpleString("OK"))
}

func (h *RedisCommandHandler) handleGet(conn net.Conn, cmd Command) error {
	if len(cmd.args) == 0 {
		return h.writeResponse(conn, ToSimpleString("ERR wrong number of arguments"))
	}

	key := cmd.args[0]
	value, exists := h.kvStore.Get(key)

	if !exists {
		return h.writeResponse(conn, []byte("$-1\r\n"))
	}

	return h.writeResponse(conn, ToBulkString(value))
}

func (h *RedisCommandHandler) handleInfo(conn net.Conn, cmd Command) error {
	if len(cmd.args) == 0 {
		return h.writeResponse(conn, ToSimpleString("ERR wrong number of arguments"))
	}

	switch strings.ToLower(cmd.args[0]) {
	case "replication":
		info := fmt.Sprintf("role:%s", h.config.Role)
		if h.config.Role == "master" {
			info += fmt.Sprintf("\nmaster_replid:%s", h.config.MasterReplicaID)
			info += fmt.Sprintf("\nmaster_repl_offset:%d", h.config.MasterReplicaOffset)
		}
		return h.writeResponse(conn, ToBulkString(info))
	default:
		return h.writeResponse(conn, ToBulkString("unsupported argument"))
	}
}

func (h *RedisCommandHandler) handleReplConf(conn net.Conn, cmd Command) error {
	if len(cmd.args) < 2 {
		return h.writeResponse(conn, ToSimpleString("ERR wrong number of arguments"))
	}

	switch strings.ToLower(cmd.args[0]) {
	case "listening-port":
		port, err := strconv.Atoi(cmd.args[1])
		if err != nil {
			h.logger.Error("Invalid port in REPLCONF: %s", cmd.args[1])
			return h.writeResponse(conn, ToSimpleString("ERR invalid port"))
		}

		if err := h.replicationManager.RegisterSlave(port, conn); err != nil {
			h.logger.Error("Failed to register slave: %v", err)
			return h.writeResponse(conn, ToSimpleString("ERR internal error"))
		}

		return h.writeResponse(conn, ToSimpleString("OK"))

	case "getack":
		// Get the actual byte offset from replication manager
		offset := strconv.Itoa(h.replicationManager.GetByteOffset())
		h.logger.Info("REPLCONF GETACK: responding with offset %s", offset)
		response := ToArray([]string{"REPLCONF", "ACK", offset})
		h.logger.Info("REPLCONF GETACK: response bytes: %v", response)
		return h.writeResponse(conn, response)
	case "ack":
		// This is an ACK response from a slave to the master
		h.logger.Info("Received REPLCONF ACK from slave: %v", cmd.args)

		// Notify the replication manager about the ACK
		h.replicationManager.NotifyAck(conn.RemoteAddr().String())

		// Master should NOT respond to ACK - it's a one-way notification
		return nil
	case "capa":
		return h.writeResponse(conn, ToSimpleString("OK"))

	default:
		return h.writeResponse(conn, ToSimpleString("ERR unknown REPLCONF option"))
	}
}

func (h *RedisCommandHandler) handlePsync(conn net.Conn, cmd Command) error {
	if len(cmd.args) < 2 {
		return h.writeResponse(conn, ToSimpleString("ERR wrong number of arguments"))
	}

	// Send FULLRESYNC response
	response := fmt.Sprintf("FULLRESYNC %s %d", h.config.MasterReplicaID, h.config.MasterReplicaOffset)
	if err := h.writeResponse(conn, ToSimpleString(response)); err != nil {
		return err
	}

	// Send empty RDB file
	decodedHex, err := hex.DecodeString(EMPTY_RDB_HEX)
	if err != nil {
		h.logger.Error("Failed to decode RDB hex: %v", err)
		return err
	}

	rdbResponse := append([]byte(fmt.Sprintf("$%d\r\n", len(decodedHex))), decodedHex...)
	return h.writeResponse(conn, rdbResponse)
}

func (h *RedisCommandHandler) handleWait(conn net.Conn, cmd Command) error {
	if len(cmd.args) < 2 {
		return h.writeResponse(conn, ToSimpleString("ERR wrong number of arguments"))
	}

	minAckSlaves, err := strconv.Atoi(cmd.args[0])
	if err != nil {
		return h.writeResponse(conn, ToSimpleString("ERR invalid number of slaves"))
	}

	waitTimeoutMs, err := strconv.Atoi(cmd.args[1])
	if err != nil {
		return h.writeResponse(conn, ToSimpleString("ERR invalid timeout"))
	}

	// Only masters can process WAIT commands
	if h.config.Role != "master" {
		return h.writeResponse(conn, ToSimpleString("ERR WAIT command can only be used on master"))
	}

	// Check if there were any write commands since the last WAIT
	lastCommand := h.commandHistory.GetLastCommand()
	if lastCommand.name == "" || !h.IsWriteCommand(lastCommand.name) {
		// No write commands to wait for - return connected slaves count immediately
		connectedSlaves := h.replicationManager.GetConnectedSlaves()
		h.logger.Info("WAIT command with no pending writes - returning connected slaves: %d", connectedSlaves)
		return h.writeResponse(conn, ToInteger(connectedSlaves))
	}

	ackedReplicas := h.replicationManager.WaitForSlaveAcknowledgments(minAckSlaves, waitTimeoutMs)
	h.logger.Info("WAIT command completed: %d replicas acknowledged", ackedReplicas)

	return h.writeResponse(conn, ToInteger(ackedReplicas))
}

func (h *RedisCommandHandler) handleConfig(conn net.Conn, cmd Command) error {
	if len(cmd.args) < 2 {
		return h.writeResponse(conn, ToSimpleString("ERR wrong number of arguments"))
	}

	switch strings.ToLower(cmd.args[0]) {
	case "get":
		switch strings.ToLower(cmd.args[1]) {
		case "dir":
			return h.writeResponse(conn, ToArray([]string{"dir", h.config.RDBDir}))
		case "dbfilename":
			return h.writeResponse(conn, ToArray([]string{"dbfilename", h.config.RDBFileName}))
		default:
			return h.writeResponse(conn, ToSimpleString("ERR unknown subcommand"))
		}
	default:
		return h.writeResponse(conn, ToSimpleString("ERR unknown subcommand"))
	}
}

func (h *RedisCommandHandler) writeResponse(conn net.Conn, response []byte) error {
	_, err := conn.Write(response)
	if err != nil {
		h.logger.Error("Failed to write response to %s: %v", conn.RemoteAddr(), err)
	}
	return err
}

func (h *RedisCommandHandler) isFromMaster(conn net.Conn) bool {
	if h.config.Role != "slave" {
		return false
	}

	// Check if this is a DummyConn (used for master commands)
	if _, isDummy := conn.(*DummyConn); isDummy {
		return true
	}

	// Check remote address for regular connections
	remoteAddr := conn.RemoteAddr()
	if remoteAddr == nil {
		return false
	}

	// Simple check - in production this would need more robust validation
	return strings.Contains(remoteAddr.String(), h.config.MasterHost)
}
