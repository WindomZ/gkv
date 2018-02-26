package sqlite

import (
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/WindomZ/gkv"
	"github.com/mattn/go-sqlite3"
)

// defaultPath is default storage file path.
var defaultPath string

// DB is mattn/go-sqlite3 adapter.
type DB struct {
	sql.DB
}

func init() {
	_, filePath, _, _ := runtime.Caller(0)
	defaultPath = filepath.Join(filepath.Dir(filepath.Dir(filePath)),
		"data", "data.db")
}

// Open creates a new sqlite3 driver by storage file path.
// paths are storage file paths.
func Open(paths ...string) gkv.KV {
	path := defaultPath
	if len(paths) != 0 {
		path = paths[0]
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		panic(sqlite3.ErrError)
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
	_, err := db.DB.Exec(fmt.Sprintf(`
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

func (db *DB) id(b []byte) string {
	h := md5.New()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// Put sets the value for a table and key.
func (db *DB) Put(table, key, value []byte) error {
	_, err := db.Exec(
		fmt.Sprintf("REPLACE INTO %s(id, k, v) VALUES (?,?,?)", string(table)),
		db.id(key),
		base64.StdEncoding.EncodeToString(key),
		base64.StdEncoding.EncodeToString(value),
	)
	return err
}

// Get retrieves the value for a table and key.
func (db *DB) Get(table, key []byte) (value []byte) {
	rows, err := db.Query(
		fmt.Sprintf("SELECT v FROM %s WHERE id=? LIMIT 1", string(table)),
		db.id(key),
	)
	if err == nil && rows.Next() {
		var s string
		err = rows.Scan(&s)
		value, _ = base64.StdEncoding.DecodeString(s)
	}
	return
}

// Count returns the total number of all the keys for a table.
func (db *DB) Count(table []byte) (i int) {
	rows, err := db.Query(
		fmt.Sprintf("SELECT COUNT(*) FROM %s LIMIT 1", string(table)),
	)
	if err == nil && rows.Next() {
		err = rows.Scan(&i)
	}
	return
}

func init() {
	gkv.Register(Open)
}
