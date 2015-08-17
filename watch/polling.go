package watch

import (
	"log"
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
	changes := NewFileChanges()
	var prevModTime time.Time

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
					changes.NotifyDeleted()
					return
				}

				if permissionErrorRetry(err, &retry) {
					continue
				}

				// XXX: report this error back to the user
				log.Fatalf("Failed to stat file %v: %v", fw.Filename, err)
			}

			// File got moved/renamed?
			if !os.SameFile(origFi, fi) {
				changes.NotifyDeleted()
				return
			}

			// File got truncated?
			fw.Size = fi.Size()
			modTime := fi.ModTime()
			if prevSize > 0 && prevSize > fw.Size {
				changes.NotifyTruncated()
				prevSize = fw.Size
				prevModTime = modTime
				continue
			} else if prevSize > 0 && prevSize == fw.Size && fw.Size <= headerSize && modTime.Sub(prevModTime) > logrotateTime {
				log.Printf("logrotateTime:%s", logrotateTime)
				changes.NotifyTruncated()
				prevSize = fw.Size
				prevModTime = modTime
				continue
			}
			prevSize = fw.Size

			// File was appended to (changed)?
			if modTime != prevModTime {
				prevModTime = modTime
				changes.NotifyModified()
			}
		}
	}()

	return changes
}

func init() {
	POLL_DURATION = 250 * time.Millisecond
}
