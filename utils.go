package gkv

import (
	"path/filepath"
	"reflect"
	"runtime"
	"unsafe"
)

// ProjectDir returns the project directory.
func ProjectDir() string {
	_, filePath, _, _ := runtime.Caller(1)
	return filepath.Dir(filepath.Dir(filePath))
}

// Btos returns the string representation of b bytes.
func Btos(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// Stob returns the bytes representation of s string.
func Stob(s string) []byte {
	return *(*[]byte)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s))))
}
