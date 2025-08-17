package main

import "fmt"

// ConfigManager handles server configuration
type ConfigManager struct {
	config *RedisServerConfig
}

// NewConfigManager creates a new configuration manager
func NewConfigManager() *ConfigManager {
	return &ConfigManager{
		config: &RedisServerConfig{
			Host:                "0.0.0.0",
			Port:                6379,
			Role:                "master",
			MasterReplicaID:     generateRandomString(40),
			MasterReplicaOffset: 0,
		},
	}
}

// GetConfig returns the current configuration
func (cm *ConfigManager) GetConfig() *RedisServerConfig {
	return cm.config
}

// SetPort sets the server port
func (cm *ConfigManager) SetPort(port int) {
	cm.config.Port = port
}

// SetAsSlave configures the server as a slave
func (cm *ConfigManager) SetAsSlave(masterHost string, masterPort int) {
	cm.config.Role = "slave"
	cm.config.MasterHost = masterHost
	cm.config.MasterPort = masterPort
}

// GetListenAddress returns the address to listen on
func (cm *ConfigManager) GetListenAddress() string {
	return fmt.Sprintf("%s:%d", cm.config.Host, cm.config.Port)
}

// generateRandomString generates a random alphanumeric string
func generateRandomString(length int) string {
	result := make([]byte, length)

	for i := 0; i < length; i++ {
		result[i] = ALPHANUMERIC_SET[i%len(ALPHANUMERIC_SET)] // Simple deterministic approach for testing
	}

	return string(result)
}
