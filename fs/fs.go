//go:build !windows
// +build !windows

// Unix file system functions.

package fs

import (
	"os"
	"syscall"
)

func isDirWR(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	if !fi.IsDir() {
		return false
	}
	if fi.Mode().Perm()&(1<<(uint(7))) == 0 {
		return false
	}
	var stat syscall.Stat_t
	if err = syscall.Stat(path, &stat); err != nil {
		return false
	}
	if uint32(os.Geteuid()) != stat.Uid {
		return false
	}
	return true
}
