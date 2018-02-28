package badger

import (
	"testing"

	"github.com/WindomZ/testify/assert"
)

var demo *KV
var (
	demoTable = []byte("table-表_1 2%3")
	demoKey   = []byte("key-键_4 5%6")
	demoValue = []byte("value-值_7 8%9")
)

func TestOpen(t *testing.T) {
	db := Open("../data/")
	if v, ok := db.(*KV); ok {
		demo = v
	}
}

func TestDB(t *testing.T) {
	assert.NotEmpty(t, demo.DB())
}

func TestRegister(t *testing.T) {
	assert.NoError(t, demo.Register(demoTable))
}

func TestPut(t *testing.T) {
	assert.NoError(t, demo.Put(demoKey, demoValue))
}

func TestGet(t *testing.T) {
	assert.Equal(t, demoValue, demo.Get(demoKey))
}

func TestCount(t *testing.T) {
	assert.Equal(t, 1, demo.Count())
}

func TestClose(t *testing.T) {
	assert.NoError(t, demo.Close())
}
