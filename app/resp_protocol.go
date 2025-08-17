package main

import "fmt"

// RESPProtocol provides utilities for RESP protocol handling
type RESPProtocol struct{}

// NewRESPProtocol creates a new RESP protocol handler
func NewRESPProtocol() *RESPProtocol {
	return &RESPProtocol{}
}

// ParseCommand parses a command from raw bytes
func (rp *RESPProtocol) ParseCommand(data []byte) (*Command, error) {
	command := ReadNextCommand(data)
	if command == nil {
		return nil, NewProtocolError("failed to parse command", nil)
	}
	return command, nil
}

// ValidateCommand validates a command structure
func (rp *RESPProtocol) ValidateCommand(cmd *Command) error {
	if cmd == nil {
		return NewProtocolError("command is nil", nil)
	}
	if cmd.name == "" {
		return NewProtocolError("command name is empty", nil)
	}
	return nil
}

// FormatError formats an error as a RESP error message
func (rp *RESPProtocol) FormatError(message string) []byte {
	return []byte(fmt.Sprintf("-ERR %s\r\n", message))
}

// FormatSimpleString formats a simple string response
func (rp *RESPProtocol) FormatSimpleString(message string) []byte {
	return ToSimpleString(message)
}

// FormatBulkString formats a bulk string response
func (rp *RESPProtocol) FormatBulkString(data string) []byte {
	return ToBulkString(data)
}

// FormatArray formats an array response
func (rp *RESPProtocol) FormatArray(data []string) []byte {
	return ToArray(data)
}

// FormatNullBulkString formats a null bulk string response
func (rp *RESPProtocol) FormatNullBulkString() []byte {
	return []byte("$-1\r\n")
}
