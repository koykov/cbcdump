package fs

import (
	"bytes"
	"io"
	"math"
	"strconv"
	"testing"

	"github.com/koykov/cbytecache"
)

func TestReader(t *testing.T) {
	r := Reader{
		FilePath: "testdata/example.bin",
		OnEOF:    KeepFile,
	}

	for {
		e, err := r.Read()
		if err == io.EOF {
			break
		}
		if !assertEntry(e) {
			t.FailNow()
		}
	}
}

func assertEntry(e cbytecache.Entry) bool {
	if len(e.Key) < 3 {
		return false
	}
	n, err := strconv.Atoi(e.Key[3:])
	if err != nil {
		return false
	}
	exp := getTestBody(n)
	if !bytes.Equal(exp, e.Body) {
		return false
	}
	if e.Expire != math.MaxUint32 {
		return false
	}
	return true
}
