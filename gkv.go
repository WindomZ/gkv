package gkv

import "errors"

// KV short for key-value,
// interface contains all behaviors for key-value adapter.
type KV interface {
	// Close releases all database resources.
	Close() error
	// Register creates a new storage if it doesn't already exist.
	Register([]byte) error
	// Put sets the value for a table and key.
	Put([]byte, []byte, []byte) error
	// Get retrieves the value for a table and key.
	Get([]byte, []byte) []byte
	// Count returns the total number of all the keys for a table.
	Count([]byte) int
}

// Instance is a function create a new KV Instance
type Instance func(paths ...string) KV

var inst Instance

// Register makes a KV adapter available by the adapter name.
// Only the last one can take effect.
func Register(i Instance) {
	inst = i
}

var db KV

// Open creates a new KV driver by table name and storage file path.
// table is the name of storage.
// paths are storage file paths.
func Open(table []byte, paths ...string) error {
	if inst == nil {
		return errors.New("forgot to import the driver")
	}
	db = inst(paths...)
	return db.Register(table)
}

// Close releases all database resources.
func Close() error {
	if db != nil {
		return db.Close()
	}
	return nil
}

// Put sets the value for a key.
func Put(table, key, value []byte) error {
	if db == nil {
		return errors.New("the db service is not started")
	}
	return db.Put(table, key, value)
}

// Get retrieves the value for a key.
func Get(table, key []byte) []byte {
	if db == nil {
		return nil
	}
	return db.Get(table, key)
}

// Count returns the total number of all the keys.
func Count(table []byte) int {
	if db == nil {
		return 0
	}
	return db.Count(table)
}
