package main

import (
	"sync"
	"time"
)

// kvEntry represents a key-value pair with optional expiration
type kvEntry struct {
	value     string
	expiresAt *time.Time
}

// MemoryKVStore implements KVStore interface using in-memory storage
type MemoryKVStore struct {
	data map[string]*kvEntry
	mu   sync.RWMutex
}

// NewMemoryKVStore creates a new in-memory KV store
func NewMemoryKVStore() *MemoryKVStore {
	return &MemoryKVStore{
		data: make(map[string]*kvEntry),
	}
}

// Set stores a key-value pair with optional expiration
func (kv *MemoryKVStore) Set(key, value string, expiry time.Duration) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry := &kvEntry{
		value: value,
	}

	if expiry > 0 {
		expiresAt := time.Now().Add(expiry)
		entry.expiresAt = &expiresAt

		// Start a goroutine to clean up expired key
		go func() {
			time.Sleep(expiry)
			kv.Delete(key)
		}()
	}

	kv.data[key] = entry
	return nil
}

// Get retrieves a value by key
func (kv *MemoryKVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	entry, exists := kv.data[key]
	if !exists {
		return "", false
	}

	// Check if key has expired
	if entry.expiresAt != nil && time.Now().After(*entry.expiresAt) {
		// Key has expired, clean it up
		go func() {
			kv.Delete(key)
		}()
		return "", false
	}

	return entry.value, true
}

// Delete removes a key from the store
func (kv *MemoryKVStore) Delete(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.data, key)
	return nil
}

// Exists checks if a key exists in the store
func (kv *MemoryKVStore) Exists(key string) bool {
	_, exists := kv.Get(key)
	return exists
}

// Cleanup removes all expired keys (can be called periodically)
func (kv *MemoryKVStore) Cleanup() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	now := time.Now()
	for key, entry := range kv.data {
		if entry.expiresAt != nil && now.After(*entry.expiresAt) {
			delete(kv.data, key)
		}
	}
}
