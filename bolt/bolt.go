package bolt

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/WindomZ/gkv"
	"github.com/boltdb/bolt"
)

// KV is boltdb/bolt adapter.
type KV struct {
	db    *bolt.DB
	table []byte
}

// Open creates a new bolt driver by storage file path.
// paths are storage file paths.
func Open(paths ...string) gkv.KV {
	var path string
	if len(paths) != 0 {
		path = paths[0]
	} else {
		path = filepath.Join(gkv.ProjectDir(), "data", "data.db")
	}
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		panic(err)
	}
	return &KV{
		db:    db,
		table: []byte(gkv.DefaultTableName),
	}
}

// DB returns the native DB of the adapter.
func (kv KV) DB() interface{} {
	return kv.db
}

// Close releases all database resources.
func (kv *KV) Close() error {
	return kv.db.Close()
}

// Register initializes a new database if it doesn't already exist.
func (kv *KV) Register(table []byte) error {
	if len(table) == 0 {
		return gkv.ErrTableName
	}
	kv.table = table
	return kv.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(table)
		if err != nil {
			return fmt.Errorf("CreateBucketIfNotExists error: %s",
				err.Error())
		}
		return nil
	})
}

// Put sets the value for a key.
func (kv *KV) Put(key, value []byte) error {
	return kv.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(kv.table).Put(key, value)
	})
}

// Get retrieves the value for a key.
func (kv *KV) Get(key []byte) (value []byte) {
	kv.db.View(func(tx *bolt.Tx) error {
		value = tx.Bucket(kv.table).Get(key)
		return nil
	})
	return
}

// Count returns the total number of all the keys.
func (kv *KV) Count() (i int) {
	kv.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(kv.table).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			i++
		}
		return nil
	})
	return
}

// Iterator creates an iterator for iterating over all the keys.
func (kv *KV) Iterator(f func([]byte, []byte) bool) error {
	return kv.db.View(func(tx *bolt.Tx) error {
		tx.Bucket(kv.table).ForEach(func(k, v []byte) error {
			if f(k, v) {
				return nil
			}
			return errors.New("stop")
		})
		return nil
	})
}

func init() {
	gkv.Register(Open)
}
