package watch

import (
	"log"
	"os"
	"time"

	"golang.org/x/net/context"
)

const (
	headerSize    = 4 * 1024
	logrotateTime = 24*time.Hour - 5*time.Minute
)

// PollingFileWatcher polls the file for changes.
type PollingFileWatcher struct {
	Filename string
	Size     int64
	ModTime  time.Time
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
	fw.ModTime = origFi.ModTime()

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
			if prevSize > 0 && prevSize > fw.Size {
				changes.NotifyTruncated()
				prevSize = fw.Size
				continue
			} else if prevSize > 0 && prevSize == fw.Size && fw.Size <= headerSize && fi.ModTime().Sub(fw.ModTime) > logrotateTime {
				log.Printf("logrotateTime:%s", logrotateTime)
				changes.NotifyTruncated()
				prevSize = fw.Size
				continue
			}
			prevSize = fw.Size

			// File was appended to (changed)?
			modTime := fi.ModTime()
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

type FileChanges struct {
	Modified  chan bool // Channel to get notified of modifications
	Truncated chan bool // Channel to get notified of truncations
	Deleted   chan bool // Channel to get notified of deletions/renames
}

func NewFileChanges() *FileChanges {
	return &FileChanges{make(chan bool), make(chan bool), make(chan bool)}
}

func (fc *FileChanges) NotifyModified() {
	sendOnlyIfEmpty(fc.Modified)
}

func (fc *FileChanges) NotifyTruncated() {
	sendOnlyIfEmpty(fc.Truncated)
}

func (fc *FileChanges) NotifyDeleted() {
	sendOnlyIfEmpty(fc.Deleted)
}

func (fc *FileChanges) Close() {
	close(fc.Modified)
	close(fc.Truncated)
	close(fc.Deleted)
}

// sendOnlyIfEmpty sends on a bool channel only if the channel has no
// backlog to be read by other goroutines. This concurrency pattern
// can be used to notify other goroutines if and only if they are
// looking for it (i.e., subsequent notifications can be compressed
// into one).
func sendOnlyIfEmpty(ch chan bool) {
	select {
	case ch <- true:
	default:
	}
}
