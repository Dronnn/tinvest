//go:build unix

package ledger

import (
	"os"

	"golang.org/x/sys/unix"
)

// lockFile takes an exclusive advisory lock (flock) on the file, blocking until
// it is available. Held across a single append so concurrent processes cannot
// interleave writes or tear a line.
func lockFile(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_EX) }

// unlockFile releases the advisory lock.
func unlockFile(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_UN) }

// fsyncDir fsyncs a directory so a newly created file's directory entry is
// durable, not just the file's own data.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
