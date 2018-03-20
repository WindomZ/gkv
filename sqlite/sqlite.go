package sqlite

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"

	"github.com/WindomZ/gkv"
	"github.com/mattn/go-sqlite3"
)

// KV is mattn/go-sqlite3 adapter.
type KV struct {
	db    *sql.DB
	table []byte
}

// Open creates a new sqlite3 driver by storage file path.
// paths are storage file paths.
func Open(paths ...string) gkv.KV {
	var path string
	if len(paths) != 0 {
		path = paths[0]
	} else {
		path = filepath.Join(gkv.ProjectDir(), "data", "data.db")
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		panic(sqlite3.ErrError)
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
	_, err := kv.db.Exec(fmt.Sprintf(`
PRAGMA foreign_keys = FALSE;
CREATE TABLE IF NOT EXISTS %s (
	id VARCHAR(34) NOT NULL,
	k TEXT NOT NULL,
	v TEXT NOT NULL,
	PRIMARY KEY (id)
);
PRAGMA foreign_keys = TRUE;
`, string(table)))
	return err
}

func (kv *KV) id(b []byte) string {
	h := md5.New()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// Put sets the value for a key.
func (kv *KV) Put(key, value []byte) error {
	_, err := kv.db.Exec(
		fmt.Sprintf("REPLACE INTO %s(id, k, v) VALUES (?,?,?)", string(kv.table)),
		kv.id(key), gkv.Btos(key), gkv.Btos(value),
	)
	return err
}

// Get retrieves the value for a key.
func (kv *KV) Get(key []byte) (value []byte) {
	rows, err := kv.db.Query(
		fmt.Sprintf("SELECT v FROM %s WHERE id=? LIMIT 1", string(kv.table)),
		kv.id(key),
	)
	if err != nil {
		return
	}
	defer rows.Close()
	if rows.Next() {
		var s string
		err = rows.Scan(&s)
		value = gkv.Stob(s)
	}
	return
}

// Delete deletes the given key from the database resources.
func (kv *KV) Delete(key []byte) error {
	_, err := kv.db.Exec(
		fmt.Sprintf("DELETE FROM %s WHERE id=?", string(kv.table)),
		kv.id(key),
	)
	return err
}

// Count returns the total number of all the keys.
func (kv *KV) Count() (i int) {
	rows, err := kv.db.Query(
		fmt.Sprintf("SELECT COUNT(*) FROM %s LIMIT 1", string(kv.table)),
	)
	if err != nil {
		return
	}
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&i)
	}
	return
}

// Iterator creates an iterator for iterating over all the keys.
func (kv *KV) Iterator(f func([]byte, []byte) bool) error {
	rows, err := kv.db.Query(
		fmt.Sprintf("SELECT k, v FROM %s", string(kv.table)),
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	var k, v []byte
	for rows.Next() {
		if err = rows.Scan(&k, &v); err == nil {
			if !f(k, v) {
				break
			}
		}
	}
	return err
}

func init() {
	gkv.Register(Open)
}
