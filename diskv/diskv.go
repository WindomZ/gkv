package diskv

import (
	"os"
	"path/filepath"

	"github.com/WindomZ/gkv"
	"github.com/peterbourgon/diskv"
)

// KV is peterbourgon/diskv adapter.
type KV struct {
	db *diskv.Diskv
}

// Open creates a new diskv driver by storage file path.
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

	db := diskv.New(diskv.Options{
		BasePath:     path,
		Transform:    func(s string) []string { return []string{} },
		CacheSizeMax: 1024 * 1024,
	})
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
	return nil
}

// Register initializes a new database if it doesn't already exist.
func (kv *KV) Register(table []byte) error {
	return nil
}

// Put sets the value for a key.
func (kv *KV) Put(key, value []byte) error {
	return kv.db.Write(gkv.Btos(key), value)
}

// Get retrieves the value for a key.
func (kv *KV) Get(key []byte) (value []byte) {
	value, _ = kv.db.Read(gkv.Btos(key))
	return
}

// Count returns the total number of all the keys.
func (kv *KV) Count() (i int) {
	for k := range kv.db.Keys(nil) {
		_, err := kv.db.Read(k)
		if err != nil {
			break
		}
		i++
	}
	return
}

// Iterator creates an iterator for iterating over all the keys.
func (kv *KV) Iterator(f func([]byte, []byte) bool) error {
	for k := range kv.db.Keys(nil) {
		v, err := kv.db.Read(k)
		if err != nil {
			return err
		}
		if !f(gkv.Stob(k), v) {
			break
		}
	}
	return nil
}

func init() {
	gkv.Register(Open)
}
