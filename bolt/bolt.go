package bolt

import (
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/WindomZ/gkv"
	"github.com/boltdb/bolt"
)

// defaultPath is default storage file path.
var defaultPath string

// DB is boltdb/bolt adapter.
type DB struct {
	bolt.DB
}

func init() {
	_, filePath, _, _ := runtime.Caller(0)
	defaultPath = filepath.Join(filepath.Dir(filepath.Dir(filePath)),
		"data", "data.db")
}

// Open creates a new bolt driver by storage file path.
// paths are storage file paths.
func Open(paths ...string) gkv.KV {
	path := defaultPath
	if len(paths) != 0 {
		path = paths[0]
	}
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		panic(err)
	}
	return &DB{
		DB: *db,
	}
}

// Close releases all database resources.
func (db *DB) Close() error {
	return db.DB.Close()
}

// Register initializes a new database if it doesn't already exist.
func (db *DB) Register(table []byte) error {
	return db.DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(table)
		if err != nil {
			return fmt.Errorf("CreateBucketIfNotExists error: %s",
				err.Error())
		}
		return nil
	})
}

// Put sets the value for a table and key.
func (db *DB) Put(table, key, value []byte) error {
	return db.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(table).Put(key, value)
	})
}

// Get retrieves the value for a table and key.
func (db *DB) Get(table, key []byte) (value []byte) {
	db.DB.View(func(tx *bolt.Tx) error {
		value = tx.Bucket(table).Get(key)
		return nil
	})
	return
}

// Count returns the total number of all the keys for a table.
func (db *DB) Count(table []byte) (i int) {
	db.DB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(table).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			i++
		}
		return nil
	})
	return
}

func init() {
	gkv.Register(Open)
}
