package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
)

// RedisConnectionManager handles network connections
type RedisConnectionManager struct {
	listener           net.Listener
	commandHandler     CommandHandler
	replicationManager ReplicationManager
	logger             Logger
	config             *RedisServerConfig
}

// NewRedisConnectionManager creates a new connection manager
func NewRedisConnectionManager(commandHandler CommandHandler, replManager ReplicationManager, logger Logger, config *RedisServerConfig) *RedisConnectionManager {
	return &RedisConnectionManager{
		commandHandler:     commandHandler,
		replicationManager: replManager,
		logger:             logger,
		config:             config,
	}
}

// Listen starts listening on the specified address
func (cm *RedisConnectionManager) Listen(address string) (net.Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to bind to address %s: %v", address, err)
	}

	cm.listener = listener
	cm.logger.Info("Listening on %s", address)
	return listener, nil
}

// HandleConnection processes a single client connection
func (cm *RedisConnectionManager) HandleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			cm.logger.Error("Error closing connection: %v", err)
		}
	}()

	cm.logger.Info("Accepted connection from %s", conn.RemoteAddr())

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				cm.logger.Info("Client closed connection: %s", conn.RemoteAddr())
				return
			}
			if cm.isConnectionResetError(err) {
				cm.logger.Info("Connection reset by client: %s", conn.RemoteAddr())
				return
			}
			cm.logger.Error("Error reading from %s: %v", conn.RemoteAddr(), err)
			return
		}

		// Parse the command
		command := ReadNextCommand(buf[:n])
		if command == nil {
			cm.logger.Error("Error parsing command from %s", conn.RemoteAddr())
			conn.Write([]byte("+PONG\r\n"))
			continue
		}

		// Handle the command
		if err := cm.processCommand(conn, *command); err != nil {
			cm.logger.Error("Error processing command %s from %s: %v", command.name, conn.RemoteAddr(), err)
			conn.Write([]byte("-ERR internal error\r\n"))
		}
	}
}

// processCommand handles a parsed command
func (cm *RedisConnectionManager) processCommand(conn net.Conn, cmd Command) error {
	// Check if handler can process this command
	if !cm.commandHandler.CanHandle(cmd.name) {
		cm.logger.Error("No handler for command: %s", cmd.name)
		return cm.writeResponse(conn, []byte("+PONG\r\n"))
	}

	// Handle the command
	if err := cm.commandHandler.Handle(conn, cmd); err != nil {
		return err
	}

	// Replicate write commands to slaves
	if cm.commandHandler.IsWriteCommand(cmd.name) && cm.config.Role == "master" {
		if err := cm.replicationManager.ReplicateCommand(cmd); err != nil {
			cm.logger.Error("Failed to replicate command %s: %v", cmd.name, err)
		}
	}

	return nil
}

// Close closes the connection manager
func (cm *RedisConnectionManager) Close() error {
	if cm.listener != nil {
		return cm.listener.Close()
	}
	return nil
}

func (cm *RedisConnectionManager) writeResponse(conn net.Conn, response []byte) error {
	_, err := conn.Write(response)
	if err != nil {
		cm.logger.Error("Failed to write response to %s: %v", conn.RemoteAddr(), err)
	}
	return err
}

func (cm *RedisConnectionManager) isConnectionResetError(err error) bool {
	// Check if the error is a network error
	if nErr, ok := err.(*net.OpError); ok {
		// Check for syscall errors in the underlying cause
		if sysErr, ok := nErr.Err.(*os.SyscallError); ok {
			if errno, ok := sysErr.Err.(syscall.Errno); ok {
				return errno == syscall.ECONNRESET
			}
		}
	}
	return false
}
