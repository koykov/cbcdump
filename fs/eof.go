package fs

// OnEOF represents function that will call on EOF.
type OnEOF func(filename string) error

// KeepFile does nothing with dump file.
func KeepFile(_ string) error {
	return nil
}
