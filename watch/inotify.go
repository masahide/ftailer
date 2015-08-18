package watch

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/masahide/fsnotify"
	"golang.org/x/net/context"
)

// InotifyFileWatcher uses inotify to monitor file changes.
type InotifyFileWatcher struct {
	Filename string
	dw       *fsnotify.Watcher
	w        *fsnotify.Watcher
	delay    time.Duration
}

func NewInotifyFileWatcher(filename string, dw, w *fsnotify.Watcher, delay time.Duration) *InotifyFileWatcher {
	fw := &InotifyFileWatcher{
		Filename: filename,
		dw:       dw,
		w:        w,
		delay:    delay,
	}
	return fw
}

func (fw *InotifyFileWatcher) BlockUntilExists(ctx context.Context) error {
	var err error
	// Do a real check now as the file might have been created before
	// calling `WatchFlags` above.
	if _, err = os.Stat(fw.Filename); !os.IsNotExist(err) {
		// file exists, or stat returned an error.
		return err
	}

	fwFilename, err := filepath.Abs(fw.Filename)
	if err != nil {
		return err
	}
	for {
		select {
		case evt, ok := <-fw.dw.Event:
			if !ok {
				return fmt.Errorf("fsnotify watcher has been closed")
			}
			evtName, err := filepath.Abs(evt.Name)
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

	go func() {
		defer fw.w.RemoveWatch(fw.Filename)
		defer changes.Close()
		var inCreate bool
		var CreateTimer <-chan time.Time
		fwFilename, err := filepath.Abs(fw.Filename)
		if err != nil {
			return
		}

		for {
			var evt *fsnotify.FileEvent
			var ok bool

			select {
			case evt, ok = <-fw.dw.Event: // ディレクトリ監視イベント
				if !ok {
					return
				}
				if !evt.IsCreate() { // IN_CREATE 以外は無視
					continue
				}
				evtName, err := filepath.Abs(evt.Name)
				if err != nil {
					return
				}
				if evtName == fwFilename {
					inCreate = true
					log.Printf("Received IN_CREATE: %s", fwFilename)
					CreateTimer = time.After(fw.delay)
					continue
				}

			case evt, ok = <-fw.w.Event: // ファイル監視イベント
				if !ok {
					return
				}
			case <-CreateTimer:
				log.Printf("IN_CREATE timeout: %s", fwFilename)
				changes.NotifyRotated() //IN_CREATEからタイムアウトしたら強制rotate
				return
			case <-ctx.Done():
				return
			}

			switch {
			case evt.IsCloseWrite():
				if inCreate {
					log.Printf("Received IN_CREATE & IN_CLOSE_WRITE: %s", fwFilename)
					changes.NotifyRotated()
					return
				}
			case evt.IsModify():
				changes.NotifyModified()
			}
		}
	}()

	return changes
}
