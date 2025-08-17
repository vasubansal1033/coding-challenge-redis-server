package main

import (
	"log"
	"os"
)

// LogLevel defines the severity of log messages
type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	ErrorLevel
	FatalLevel
)

// DefaultLogger implements the Logger interface using Go's standard log package
type DefaultLogger struct {
	level  LogLevel
	logger *log.Logger
}

// NewDefaultLogger creates a new default logger
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		level:  level,
		logger: log.New(os.Stdout, "", log.LstdFlags),
	}
}

// Debug logs a debug message
func (l *DefaultLogger) Debug(msg string, args ...interface{}) {
	if l.level <= DebugLevel {
		l.logger.Printf("[DEBUG] "+msg, args...)
	}
}

// Info logs an info message
func (l *DefaultLogger) Info(msg string, args ...interface{}) {
	if l.level <= InfoLevel {
		l.logger.Printf("[INFO] "+msg, args...)
	}
}

// Error logs an error message
func (l *DefaultLogger) Error(msg string, args ...interface{}) {
	if l.level <= ErrorLevel {
		l.logger.Printf("[ERROR] "+msg, args...)
	}
}

// Fatal logs a fatal message and exits
func (l *DefaultLogger) Fatal(msg string, args ...interface{}) {
	l.logger.Fatalf("[FATAL] "+msg, args...)
}
