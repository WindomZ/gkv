package buntdb

import (
	"path/filepath"

	"github.com/WindomZ/gkv"
	"github.com/tidwall/buntdb"
)

// KV is tidwall/buntdb adapter.
type KV struct {
	db *buntdb.DB
}

// Open creates a new buntdb driver by storage file path.
// paths are storage file paths.
func Open(paths ...string) gkv.KV {
	var path string
	if len(paths) != 0 {
		path = paths[0]
	} else {
		path = filepath.Join(gkv.ProjectDir(), "data", "data.db")
	}
	db, err := buntdb.Open(path)
	if err != nil {
		panic(err)
	}
	return &KV{
		db: db,
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
	return nil
}

// Put sets the value for a key.
func (kv *KV) Put(key, value []byte) error {
	return kv.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(gkv.Btos(key), gkv.Btos(value), nil)
		return err
	})
}

// Get retrieves the value for a key.
func (kv *KV) Get(key []byte) (value []byte) {
	kv.db.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get(gkv.Btos(key))
		if err != nil {
			return err
		}
		value = gkv.Stob(val)
		return nil
	})
	return
}

// Delete deletes the given key from the database resources.
func (kv *KV) Delete(key []byte) error {
	return kv.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(gkv.Btos(key))
		return err
	})
}

// Count returns the total number of all the keys.
func (kv *KV) Count() (i int) {
	kv.db.View(func(tx *buntdb.Tx) error {
		err := tx.Ascend("", func(key, value string) bool {
			i++
			return true
		})
		return err
	})
	return
}

// Iterator creates an iterator for iterating over all the keys.
func (kv *KV) Iterator(f func([]byte, []byte) bool) error {
	return kv.db.View(func(tx *buntdb.Tx) error {
		err := tx.Ascend("", func(key, value string) bool {
			return f(gkv.Stob(key), gkv.Stob(value))
		})
		return err
	})
}

func init() {
	gkv.Register(Open)
}
