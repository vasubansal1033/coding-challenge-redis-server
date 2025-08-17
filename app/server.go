package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

// Main function for the Redis server

func main() {
	// Parse command line flags
	redisPort := flag.Int("port", REDIS_PORT, "port on which redis server will run")
	masterAddress := flag.String("replicaof", "", "address and port of master replica")
	flag.Parse()

	// Create and configure the Redis server
	server := NewRedisServer()
	server.SetPort(*redisPort)

	// Configure as slave if master address is provided
	if *masterAddress != "" {
		masterAddr := strings.Split(*masterAddress, " ")
		if len(masterAddr) != 2 {
			fmt.Fprintf(os.Stderr, "Invalid master address format: %s\n", *masterAddress)
			os.Exit(1)
		}

		masterHost := masterAddr[0]
		masterPort, err := strconv.Atoi(masterAddr[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid master port: %s\n", masterAddr[1])
			os.Exit(1)
		}

		server.SetAsSlave(masterHost, masterPort)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the server
	if err := server.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start server: %v\n", err)
		os.Exit(1)
	}

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("\nReceived shutdown signal, stopping server...")

	// Stop the server gracefully
	if err := server.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "Error stopping server: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Server stopped successfully")
}
