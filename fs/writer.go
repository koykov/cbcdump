package fs

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/koykov/bytealg"
	"github.com/koykov/cbytecache"
	"github.com/koykov/clock"
	"github.com/koykov/fastconv"
)

const (
	flushChunkSize = 16
)

// Writer represent filesystem writer implementation.
type Writer struct {
	// Max buffer size in bytes.
	// Writer will move buffered data to destination file on overflow.
	Buffer cbytecache.MemorySize
	// Write filepath.
	// Supports strftime patterns (see https://github.com/koykov/clock#format) if versioning required.
	// By design, it is static file name.
	FilePath string

	once sync.Once
	bs   cbytecache.MemorySize
	fp   string
	fd   string
	ft   string

	mux sync.Mutex
	f   *os.File
	buf []byte

	err error
}

// Write writes entry to binary file.
//
// It returns the number of bytes written from entry and any error encountered.
func (w *Writer) Write(entry cbytecache.Entry) (n int, err error) {
	w.once.Do(w.init)
	if w.err != nil {
		err = w.err
		return
	}

	w.mux.Lock()
	defer w.mux.Unlock()

	off := len(w.buf)
	poff := off
	w.buf = bytealg.GrowDelta(w.buf, 2)
	binary.LittleEndian.PutUint16(w.buf[off:], uint16(len(entry.Key)))
	off += 2
	w.buf = append(w.buf, entry.Key...)
	off += len(entry.Key)

	w.buf = bytealg.GrowDelta(w.buf, 4)
	binary.LittleEndian.PutUint32(w.buf[off:], uint32(len(entry.Body)))
	off += 4
	w.buf = append(w.buf, entry.Body...)
	off += len(entry.Body)

	w.buf = bytealg.GrowDelta(w.buf, 4)
	binary.LittleEndian.PutUint32(w.buf[off:], entry.Expire)
	off += 4

	n = off - poff

	if cbytecache.MemorySize(len(w.buf)) >= w.bs {
		err = w.flushBuf()
	}

	return
}

// Flush flushes all buffered data to binary file and close it.
func (w *Writer) Flush() (err error) {
	w.once.Do(w.init)
	if w.err != nil {
		return w.err
	}

	w.mux.Lock()
	defer w.mux.Unlock()

	if len(w.buf) > 0 {
		if err = w.flushBuf(); err != nil {
			return
		}
	}

	if err = w.f.Close(); err != nil {
		return
	}
	err = os.Rename(w.ft, w.fd)
	w.f = nil

	return
}

// Close is an alias of Flush() method.
func (w *Writer) Close() error {
	return w.Flush()
}

func (w *Writer) init() {
	w.err = nil
	if len(w.FilePath) == 0 {
		w.err = ErrNoFilePath
		return
	}
	dir := filepath.Dir(w.FilePath)
	if !isDirWR(dir) {
		w.err = ErrDirNoWR
		return
	}

	w.fp = w.FilePath
	if w.bs = w.Buffer; w.bs > 0 {
		w.buf = make([]byte, 0, w.bs)
	}
}

func (w *Writer) flushBuf() (err error) {
	if w.f == nil {
		buf := make([]byte, 0, len(w.fp)*2)
		if buf, err = clock.AppendFormat(buf, w.fp, time.Now()); err != nil {
			return
		}
		w.fd = fastconv.B2S(buf)
		w.ft = w.fd + ".tmp"
		if w.f, err = os.Create(w.ft); err != nil {
			return
		}
	}

	p := w.buf
	for len(p) >= flushChunkSize {
		if _, err = w.f.Write(p[:flushChunkSize]); err != nil {
			return
		}
		p = p[flushChunkSize:]
	}
	if len(p) > 0 {
		if _, err = w.f.Write(p); err != nil {
			return
		}
	}
	w.buf = w.buf[:0]
	return
}
