package main

import (
	"fmt"
)

// RedisError represents a Redis-specific error
type RedisError struct {
	Code    string
	Message string
	Cause   error
}

func (e *RedisError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("Redis error [%s]: %s (caused by: %v)", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("Redis error [%s]: %s", e.Code, e.Message)
}

func (e *RedisError) Unwrap() error {
	return e.Cause
}

// Error codes
const (
	ErrCodeInvalidCommand   = "INVALID_COMMAND"
	ErrCodeWrongArguments   = "WRONG_ARGUMENTS"
	ErrCodeKeyNotFound      = "KEY_NOT_FOUND"
	ErrCodeInternalError    = "INTERNAL_ERROR"
	ErrCodeConnectionError  = "CONNECTION_ERROR"
	ErrCodeReplicationError = "REPLICATION_ERROR"
	ErrCodeConfigError      = "CONFIG_ERROR"
	ErrCodeProtocolError    = "PROTOCOL_ERROR"
)

// NewRedisError creates a new Redis error
func NewRedisError(code, message string, cause error) *RedisError {
	return &RedisError{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// Common error constructors
func NewInvalidCommandError(command string) *RedisError {
	return NewRedisError(ErrCodeInvalidCommand, fmt.Sprintf("unknown command '%s'", command), nil)
}

func NewWrongArgumentsError(command string) *RedisError {
	return NewRedisError(ErrCodeWrongArguments, fmt.Sprintf("wrong number of arguments for '%s' command", command), nil)
}

func NewKeyNotFoundError(key string) *RedisError {
	return NewRedisError(ErrCodeKeyNotFound, fmt.Sprintf("key '%s' not found", key), nil)
}

func NewInternalError(message string, cause error) *RedisError {
	return NewRedisError(ErrCodeInternalError, message, cause)
}

func NewConnectionError(message string, cause error) *RedisError {
	return NewRedisError(ErrCodeConnectionError, message, cause)
}

func NewReplicationError(message string, cause error) *RedisError {
	return NewRedisError(ErrCodeReplicationError, message, cause)
}

func NewConfigError(message string, cause error) *RedisError {
	return NewRedisError(ErrCodeConfigError, message, cause)
}

func NewProtocolError(message string, cause error) *RedisError {
	return NewRedisError(ErrCodeProtocolError, message, cause)
}

// ErrorHandler provides utilities for handling errors
type ErrorHandler struct {
	logger Logger
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(logger Logger) *ErrorHandler {
	return &ErrorHandler{
		logger: logger,
	}
}

// HandleError logs and optionally returns a formatted error response
func (eh *ErrorHandler) HandleError(err error, context string) string {
	if redisErr, ok := err.(*RedisError); ok {
		eh.logger.Error("%s: %s", context, redisErr.Error())
		return fmt.Sprintf("-ERR %s\r\n", redisErr.Message)
	}

	eh.logger.Error("%s: %v", context, err)
	return "-ERR internal error\r\n"
}

// LogError logs an error with context
func (eh *ErrorHandler) LogError(err error, context string) {
	if redisErr, ok := err.(*RedisError); ok {
		eh.logger.Error("%s: [%s] %s", context, redisErr.Code, redisErr.Message)
		if redisErr.Cause != nil {
			eh.logger.Error("Caused by: %v", redisErr.Cause)
		}
	} else {
		eh.logger.Error("%s: %v", context, err)
	}
}

// IsRetryableError checks if an error is retryable
func (eh *ErrorHandler) IsRetryableError(err error) bool {
	if redisErr, ok := err.(*RedisError); ok {
		return redisErr.Code == ErrCodeConnectionError || redisErr.Code == ErrCodeInternalError
	}
	return false
}

// IsClientError checks if an error is a client error (400-level)
func (eh *ErrorHandler) IsClientError(err error) bool {
	if redisErr, ok := err.(*RedisError); ok {
		return redisErr.Code == ErrCodeInvalidCommand ||
			redisErr.Code == ErrCodeWrongArguments ||
			redisErr.Code == ErrCodeKeyNotFound ||
			redisErr.Code == ErrCodeProtocolError
	}
	return false
}

// RecoverFromPanic recovers from panics and converts them to errors
func (eh *ErrorHandler) RecoverFromPanic() error {
	if r := recover(); r != nil {
		eh.logger.Error("Panic recovered: %v", r)
		return NewInternalError("internal panic occurred", fmt.Errorf("%v", r))
	}
	return nil
}
