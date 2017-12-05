package watch

import (
	"context"
	"log"
	"os"
	"time"
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
}

/*
func (fw *PollingFileWatcher) ChangeEvents(ctx context.Context, origFi os.FileInfo) *FileChanges {
	// TODO: Not implement.
	changes := NewFileChanges()
	return changes
}
*/

func (fw *PollingFileWatcher) ChangeEvents(ctx context.Context, origFi os.FileInfo) *FileChanges {
	changes := NewFileChanges()
	var prevModTime time.Time

	// XXX: use tomb.Tomb to cleanly manage these goroutines. replace
	// the fatal (below) with tomb's Kill.

	fw.Size = origFi.Size()

	go func() {
		defer changes.Close()

		var retry int = 0

		prevSize := fw.Size
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			time.Sleep(POLL_DURATION)
			fi, err := os.Stat(fw.Filename)
			if err != nil {
				if os.IsNotExist(err) {
					// File does not exist (has been deleted).
					changes.NotifyRotated(ctx)
					return
				}

				if permissionErrorRetry(err, &retry) {
					continue
				}

				// XXX: report this error back to the user
				log.Printf("Failed to stat file %v: %v", fw.Filename, err)
			}

			// File got moved/renamed?
			if !os.SameFile(origFi, fi) {
				changes.NotifyRotated(ctx)
				return
			}

			// File got truncated?
			fw.Size = fi.Size()
			if prevSize > 0 && prevSize > fw.Size {
				changes.NotifyRotated(ctx)
				prevSize = fw.Size
				continue
			}
			prevSize = fw.Size

			// File was appended to (changed)?
			modTime := fi.ModTime()
			if modTime != prevModTime {
				prevModTime = modTime
				changes.NotifyModified(ctx)
			}
		}
	}()

	return changes
}

func init() {
	POLL_DURATION = 250 * time.Millisecond
}
