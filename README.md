# gkv

> gkv is an embeddable, persistent, simple key/value(KV) database adapter for Go.

[![Build Status](https://travis-ci.org/WindomZ/gkv.svg?branch=master)](https://travis-ci.org/WindomZ/gkv)

This package depends on third-party [databases](#databases).

## Features
- Out of the box, easy to use.
- Basic, common, pure Go.
- Support multiple [databases](#databases).

## Databases
- [x] [bolt](https://github.com/WindomZ/gkv/tree/master/bolt) - an embedded key/value database for Go.[[GitHub]](https://github.com/boltdb/bolt)
- [x] [badger](https://github.com/WindomZ/gkv/tree/master/badger) - fast key-value DB in Go.[[GitHub]](https://github.com/dgraph-io/badger)
- [x] [leveldb](https://github.com/WindomZ/gkv/tree/master/leveldb) - key/value database in Go.[[GitHub]](https://github.com/syndtr/goleveldb)
- [x] [buntdb](https://github.com/WindomZ/gkv/tree/master/buntdb) - an embeddable, in-memory key/value database for Go with custom indexing and geospatial support.[[GitHub]](https://github.com/tidwall/buntdb)
- [x] [sqlite3](https://github.com/WindomZ/gkv/tree/master/sqlite) - sqlite3 driver for go using database/sql.[[GitHub]](https://github.com/mattn/go-sqlite3)

## Installing
```bash
go get -u github.com/WindomZ/gkv/...
```

## Usage
```
import (
	"github.com/WindomZ/gkv"
	_ "github.com/WindomZ/gkv/bolt"
)
...

// init db
db := Open("../data/bolt.db")
db.Register([]byte("tablename"))
...

// put the value for a key
db.Put([]byte("key1"), []byte("value1"))
db.Put([]byte("key2"), []byte("value2"))
...

// get the value for a key
demo.Get([]byte("key1"))
demo.Get([]byte("key2"))
```

## Contributing
Welcome to pull requests, report bugs, suggest ideas and discuss on [issues page](https://github.com/WindomZ/gkv/issues).

If you like it then you can put a :star: on it.
