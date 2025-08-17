package main

import (
	"context"
	"net"
	"sync"
)

// RedisServerImpl implements the RedisServer interface
type RedisServerImpl struct {
	config             *RedisServerConfig
	configManager      *ConfigManager
	kvStore            KVStore
	commandHandler     CommandHandler
	connectionManager  ConnectionManager
	replicationManager ReplicationManager
	logger             Logger
	errorHandler       *ErrorHandler
	respProtocol       *RESPProtocol

	listener net.Listener
	shutdown chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex
}

// NewRedisServer creates a new Redis server instance
func NewRedisServer() *RedisServerImpl {
	// Initialize configuration
	configManager := NewConfigManager()
	config := configManager.GetConfig()

	// Initialize logger
	logger := NewDefaultLogger(InfoLevel)

	// Initialize error handler
	errorHandler := NewErrorHandler(logger)

	// Initialize RESP protocol handler
	respProtocol := NewRESPProtocol()

	// Initialize KV store
	kvStore := NewMemoryKVStore()

	// Initialize replication manager
	replicationManager := NewRedisReplicationManager(config, logger)

	// Initialize command handler
	commandHandler := NewRedisCommandHandler(kvStore, config, replicationManager, logger)

	// Set the command handler on the replication manager so it can process master commands
	replicationManager.SetCommandHandler(commandHandler)

	// Initialize connection manager
	connectionManager := NewRedisConnectionManager(commandHandler, replicationManager, logger, config)

	return &RedisServerImpl{
		config:             config,
		configManager:      configManager,
		kvStore:            kvStore,
		commandHandler:     commandHandler,
		connectionManager:  connectionManager,
		replicationManager: replicationManager,
		logger:             logger,
		errorHandler:       errorHandler,
		respProtocol:       respProtocol,
		shutdown:           make(chan struct{}),
	}
}

// Start starts the Redis server
func (s *RedisServerImpl) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil {
		return NewConfigError("server already started", nil)
	}

	// Start listening
	address := s.configManager.GetListenAddress()
	listener, err := s.connectionManager.Listen(address)
	if err != nil {
		return NewConnectionError("failed to start listening", err)
	}

	s.listener = listener
	s.logger.Info("Redis server started on %s", address)

	// If this is a slave, initiate connection to master
	if s.config.Role == "slave" {
		go func() {
			if err := s.replicationManager.HandleMasterHandshake(); err != nil {
				s.logger.Error("Failed to connect to master: %v", err)
			}
		}()
	}

	// Start accepting connections
	go s.acceptConnections(ctx)

	return nil
}

// Stop stops the Redis server
func (s *RedisServerImpl) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return nil // Already stopped
	}

	s.logger.Info("Stopping Redis server...")

	// Signal shutdown
	close(s.shutdown)

	// Close listener
	if err := s.listener.Close(); err != nil {
		s.logger.Error("Error closing listener: %v", err)
	}
	s.listener = nil

	// Wait for all connections to finish
	s.wg.Wait()

	// Close connection manager
	if err := s.connectionManager.Close(); err != nil {
		s.logger.Error("Error closing connection manager: %v", err)
	}

	s.logger.Info("Redis server stopped")
	return nil
}

// HandleConnection handles a single connection (implements RedisServer interface)
func (s *RedisServerImpl) HandleConnection(conn net.Conn) {
	s.connectionManager.HandleConnection(conn)
}

// GetConfig returns the server configuration
func (s *RedisServerImpl) GetConfig() *RedisServerConfig {
	return s.config
}

// SetPort sets the server port
func (s *RedisServerImpl) SetPort(port int) {
	s.configManager.SetPort(port)
}

// SetAsSlave configures the server as a slave
func (s *RedisServerImpl) SetAsSlave(masterHost string, masterPort int) {
	s.configManager.SetAsSlave(masterHost, masterPort)
}

// acceptConnections handles incoming connections
func (s *RedisServerImpl) acceptConnections(ctx context.Context) {
	defer func() {
		if err := s.errorHandler.RecoverFromPanic(); err != nil {
			s.logger.Error("Panic in acceptConnections: %v", err)
		}
	}()

	for {
		select {
		case <-s.shutdown:
			return
		case <-ctx.Done():
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return // Shutdown initiated
			default:
				s.logger.Error("Error accepting connection: %v", err)
				continue
			}
		}

		// Handle connection in a separate goroutine
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer func() {
				if err := s.errorHandler.RecoverFromPanic(); err != nil {
					s.logger.Error("Panic in connection handler: %v", err)
				}
			}()

			s.HandleConnection(c)
		}(conn)
	}
}

// GetKVStore returns the KV store (for testing purposes)
func (s *RedisServerImpl) GetKVStore() KVStore {
	return s.kvStore
}

// GetReplicationManager returns the replication manager (for testing purposes)
func (s *RedisServerImpl) GetReplicationManager() ReplicationManager {
	return s.replicationManager
}

// IsRunning returns true if the server is currently running
func (s *RedisServerImpl) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.listener != nil
}
