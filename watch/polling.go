package watch

import (
	"os"
	"time"

	"golang.org/x/net/context"
)

// PollingFileWatcher polls the file for changes.
type PollingFileWatcher struct {
	Filename string
	Size     int64
}

func NewPollingFileWatcher(filename string) *PollingFileWatcher {
	fw := &PollingFileWatcher{Filename: filename, Size: 0}
	return fw
}

var POLL_DURATION time.Duration

func (fw *PollingFileWatcher) BlockUntilExists(ctx context.Context) error {
	for {
		if _, err := os.Stat(fw.Filename); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-time.After(POLL_DURATION):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	panic("unreachable")
}

func (fw *PollingFileWatcher) ChangeEvents(ctx context.Context, origFi os.FileInfo) *FileChanges {
	// TODO: Not implement.
	changes := NewFileChanges()
	return changes
}

func init() {
	POLL_DURATION = 250 * time.Millisecond
}
