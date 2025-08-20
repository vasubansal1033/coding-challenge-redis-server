package main

import (
	"context"
	"net"
	"time"
)

// RedisServer interface defines the core Redis server operations
type RedisServer interface {
	Start(ctx context.Context) error
	Stop() error
	HandleConnection(conn net.Conn)
	GetConfig() *RedisServerConfig
}

// RedisServerConfig holds all server configuration
type RedisServerConfig struct {
	Host                string
	Port                int
	Role                string
	MasterHost          string
	MasterPort          int
	MasterReplicaID     string
	MasterReplicaOffset int
}

// KVStore interface for key-value storage operations
type KVStore interface {
	Set(key, value string, expiry time.Duration) error
	Get(key string) (string, bool)
	Delete(key string) error
	Exists(key string) bool
}

// CommandHandler interface for handling Redis commands
type CommandHandler interface {
	Handle(conn net.Conn, cmd Command) error
	CanHandle(commandName string) bool
	IsWriteCommand(commandName string) bool
}

// ReplicationManager interface for master-slave replication
type ReplicationManager interface {
	GetConnectedSlaves() int
	RegisterSlave(port int, conn net.Conn) error
	ReplicateCommand(cmd Command) error
	StartSlaveConnection() (net.Conn, error)
	HandleMasterHandshake() error
	GetByteOffset() int
	SetCommandHandler(handler CommandHandler)
	WaitForSlaveAcknowledgments(numSlaves int, timeoutMs int) int
	NotifyAck(slaveAddress string)
}

// ConnectionManager interface for handling network connections
type ConnectionManager interface {
	Listen(address string) (net.Listener, error)
	HandleConnection(conn net.Conn)
	Close() error
}

// Logger interface for logging operations
type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
	Fatal(msg string, args ...interface{})
}
