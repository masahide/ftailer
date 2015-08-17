package watch

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/net/context"

	//"gopkg.in/fsnotify.v0"
	"golang.org/x/exp/inotify"
)

// InotifyFileWatcher uses inotify to monitor file changes.
type InotifyFileWatcher struct {
	Filename string
	Size     int64
	w        *inotify.Watcher
	ModTime  time.Time
}

func NewInotifyFileWatcher(filename string, w *inotify.Watcher) *InotifyFileWatcher {
	fw := &InotifyFileWatcher{filename, 0, w, time.Time{}}
	return fw
}

func (fw *InotifyFileWatcher) BlockUntilExists(ctx context.Context) error {
	dirname := filepath.Dir(fw.Filename)

	// Watch for new files to be created in the parent directory.
	err := fw.w.AddWatch(dirname, inotify.IN_ONLYDIR)
	if err != nil {
		return err
	}
	defer fw.w.RemoveWatch(dirname)

	// Do a real check now as the file might have been created before
	// calling `WatchFlags` above.
	if _, err = os.Stat(fw.Filename); !os.IsNotExist(err) {
		// file exists, or stat returned an error.
		return err
	}

	for {
		select {
		case evt, ok := <-fw.w.Event:
			if !ok {
				return fmt.Errorf("inotify watcher has been closed")
			}
			evtName, err := filepath.Abs(evt.Name)
			if err != nil {
				return err
			}
			fwFilename, err := filepath.Abs(fw.Filename)
			if err != nil {
				return err
			}
			if evtName == fwFilename {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	panic("unreachable")
}

func (fw *InotifyFileWatcher) ChangeEvents(ctx context.Context, fi os.FileInfo) *FileChanges {
	changes := NewFileChanges()

	err := fw.w.Watch(fw.Filename)
	if err != nil {
		log.Fatalf("Error watching %v: %v", fw.Filename, err)
	}

	fw.Size = fi.Size()
	fw.ModTime = fi.ModTime()

	go func() {
		defer fw.w.RemoveWatch(fw.Filename)
		defer changes.Close()

		for {
			prevSize := fw.Size

			var evt *inotify.Event
			var ok bool

			select {
			case evt, ok = <-fw.w.Event:
				if !ok {
					return
				}
			case <-ctx.Done():
				return
			}

			switch {
			//case evt.IsDelete():
			case evt.Mask&inotify.IN_DELETE_SELF != 0:
				fallthrough

			//case evt.IsRename():
			case evt.Mask&inotify.IN_MOVE_SELF != 0:
				changes.NotifyDeleted()
				continue
				//return
			//case evt.IsRename():
			case evt.Mask&inotify.IN_CLOSE_WRITE != 0:
				changes.NotifyClosed()
				return

			//case evt.IsModify():
			case evt.Mask&inotify.IN_MODIFY != 0:
				fi, err := os.Stat(fw.Filename)
				if err != nil {
					if os.IsNotExist(err) {
						changes.NotifyDeleted()
						return
					}
					// XXX: report this error back to the user
					log.Fatalf("Failed to stat file %v: %v", fw.Filename, err)
				}
				fw.Size = fi.Size()

				if prevSize > 0 && prevSize > fw.Size {
					log.Printf("prevSize:%d,fw.Size:%d", prevSize, fw.Size)
					changes.NotifyTruncated()
					return
				} else {
					changes.NotifyModified()
				}
				prevSize = fw.Size
			}
		}
	}()

	return changes
}
