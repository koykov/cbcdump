package fs

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/koykov/bytealg"
	"github.com/koykov/cbytecache"
	"github.com/koykov/fastconv"
)

// Reader represent filesystem reader implementation.
type Reader struct {
	// FilePath represents dumpfile path.
	// Mandatory param.
	FilePath string
	// OnEOF calls when EOF of current file reaches.
	// If this param omit os.Remove() will use by default.
	OnEOF OnEOF

	once sync.Once
	fp   string
	eof  func(string) error

	mux sync.Mutex
	f   *os.File
	buf []byte
}

// Read reads entry from binary file.
//
// It returns entry and any error encountered.
func (r *Reader) Read() (e cbytecache.Entry, err error) {
	r.once.Do(r.init)

	r.mux.Lock()
	defer func() {
		err = r.checkEOF(err)
		r.mux.Unlock()
	}()

	if r.f == nil {
		if r.f, err = os.OpenFile(r.fp, os.O_RDONLY, 0644); err != nil {
			return
		}
	}

	r.buf = r.buf[:0]
	off := 0
	r.buf = bytealg.GrowDelta(r.buf, 2)
	if _, err = io.ReadAtLeast(r.f, r.buf[off:], 2); err != nil {
		return
	}
	l := int(binary.LittleEndian.Uint16(r.buf[off:]))
	off += 2
	klo := off
	r.buf = bytealg.GrowDelta(r.buf, l)
	if _, err = io.ReadAtLeast(r.f, r.buf[off:], l); err != nil {
		return
	}
	off += l
	khi := off

	r.buf = bytealg.GrowDelta(r.buf, 4)
	if _, err = io.ReadAtLeast(r.f, r.buf[off:], 4); err != nil {
		return
	}
	l = int(binary.LittleEndian.Uint32(r.buf[off:]))
	off += 4
	blo := off
	r.buf = bytealg.GrowDelta(r.buf, l)
	if _, err = io.ReadAtLeast(r.f, r.buf[off:], l); err != nil {
		return
	}
	off += l
	bhi := off

	r.buf = bytealg.GrowDelta(r.buf, 4)
	if _, err = io.ReadAtLeast(r.f, r.buf[off:], 4); err != nil {
		return
	}
	e.Expire = binary.LittleEndian.Uint32(r.buf[off:])
	off += 4

	e.Key = fastconv.B2S(r.buf[klo:khi])
	e.Body = r.buf[blo:bhi]

	return
}

func (r *Reader) init() {
	r.fp = r.FilePath
	if r.OnEOF == nil {
		r.OnEOF = os.Remove
	}
	r.eof = r.OnEOF
}

func (r *Reader) checkEOF(err error) error {
	if err == io.EOF {
		_ = r.f.Close()
		_ = r.eof(r.fp)
		r.fp = ""
		r.f = nil
	}
	return err
}
