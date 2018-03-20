package leveldb

import (
	"path/filepath"

	"github.com/WindomZ/gkv"
	"github.com/syndtr/goleveldb/leveldb"
)

// KV is a goleveldb/leveldb adapter.
type KV struct {
	db *leveldb.DB
}

// Open creates a new leveldb driver by storage file path.
// paths are storage file paths.
func Open(paths ...string) gkv.KV {
	var path string
	if len(paths) != 0 {
		path = paths[0]
	} else {
		path = filepath.Join(gkv.ProjectDir(), "data", "leveldb")
	}
	db, err := leveldb.OpenFile(path, nil)
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
	return kv.db.Put(key, value, nil)
}

// Get retrieves the value for a key.
func (kv *KV) Get(key []byte) (value []byte) {
	value, _ = kv.db.Get(key, nil)
	return
}

// Delete deletes the given key from the database resources.
func (kv *KV) Delete(key []byte) error {
	return kv.db.Delete(key, nil)
}

// Count returns the total number of all the keys.
func (kv *KV) Count() (i int) {
	iter := kv.db.NewIterator(nil, nil)
	for iter.Next() {
		i++
	}
	iter.Release()
	return
}

// Iterator creates an iterator for iterating over all the keys.
func (kv *KV) Iterator(f func([]byte, []byte) bool) error {
	iter := kv.db.NewIterator(nil, nil)
	for iter.Next() {
		if !f(iter.Key(), iter.Value()) {
			break
		}
	}
	iter.Release()
	return iter.Error()
}

func init() {
	gkv.Register(Open)
}
