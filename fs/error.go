package fs

import "errors"

var (
	ErrNoFilePath = errors.New("no filepath provided")
	ErrDirNoWR    = errors.New("directory doesn't exists or writable")
)
