// Windows file system functions.

package fs

import "os"

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
	return true
}
