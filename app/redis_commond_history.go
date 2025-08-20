package main

import "sync"

type CommandHistory struct {
	commands []Command
	mu       sync.RWMutex
}

func NewCommandHistory() *CommandHistory {
	return &CommandHistory{
		commands: make([]Command, 0, COMMAND_HISTORY_SIZE),
	}
}

func (ch *CommandHistory) AddCommand(cmd Command) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.commands = append(ch.commands, cmd)
}

func (ch *CommandHistory) GetCommands() []Command {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	return ch.commands
}

func (ch *CommandHistory) GetLastCommand() Command {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.commands) == 0 {
		return Command{}
	}

	return ch.commands[len(ch.commands)-1]
}
