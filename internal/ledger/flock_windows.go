//go:build windows

package ledger

import (
	"os"

	"golang.org/x/sys/windows"
)

// lockFile takes a blocking exclusive lock over the journal file. All tinvest
// processes lock the same range before appending, preventing interleaved lines.
func lockFile(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK,
		0,
		^uint32(0),
		^uint32(0),
		&overlapped,
	)
}

// unlockFile releases the journal lock range.
func unlockFile(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(
		windows.Handle(f.Fd()),
		0,
		^uint32(0),
		^uint32(0),
		&overlapped,
	)
}

// Windows does not expose a directory fsync equivalent for ordinary directory
// handles. The journal file itself is still flushed after every append.
func fsyncDir(string) error { return nil }
