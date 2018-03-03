package badger

import (
	"os"
	"path/filepath"

	"github.com/WindomZ/gkv"
	"github.com/dgraph-io/badger"
)

// KV is dgraph-io/badger adapter.
type KV struct {
	db *badger.DB
}

// Open creates a new badger driver by storage file path.
// paths are storage file paths.
func Open(paths ...string) gkv.KV {
	var path string
	if len(paths) != 0 {
		path = paths[0]
	} else {
		path = filepath.Join(gkv.ProjectDir(), "data")
	}
	f, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(path, 0755)
		}
		if err != nil {
			panic(err)
		}
	} else if !f.IsDir() {
		path = filepath.Dir(path)
	}

	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	opts.MaxTableSize = 1 << 15
	opts.LevelOneSize = 4 << 15
	opts.SyncWrites = false

	db, err := badger.Open(opts)
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
	return kv.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get retrieves the value for a key.
func (kv *KV) Get(key []byte) (value []byte) {
	kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.Value()
		return err
	})
	return
}

// Count returns the total number of all the keys.
func (kv *KV) Count() (i int) {
	kv.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		for it.Rewind(); it.Valid(); it.Next() {
			i++
		}
		it.Close()
		return nil
	})
	return
}

func init() {
	gkv.Register(Open)
}
