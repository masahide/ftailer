package watch

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/exp/inotify"
	"golang.org/x/net/context"
)

// InotifyFileWatcher uses inotify to monitor file changes.
type InotifyFileWatcher struct {
	Filename string
	dw       *inotify.Watcher
	w        *inotify.Watcher
	delay    time.Duration
}

func NewInotifyFileWatcher(filename string, dw, w *inotify.Watcher, delay time.Duration) *InotifyFileWatcher {
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
				return fmt.Errorf("inotify watcher has been closed")
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
		var mask uint32
		var CreateTimer <-chan time.Time
		fwFilename, err := filepath.Abs(fw.Filename)
		if err != nil {
			return
		}

		for {
			var evt *inotify.Event
			var ok bool

			select {
			case evt, ok = <-fw.dw.Event: // ディレクトリ監視イベント
				if !ok {
					return
				}
				if evt.Mask&inotify.IN_CREATE == 0 { // IN_CREATE 以外は無視
					continue
				}
				evtName, err := filepath.Abs(evt.Name)
				if err != nil {
					return
				}
				if evtName != fwFilename {
					mask |= inotify.IN_CREATE
					CreateTimer = time.After(5 * time.Second)
					continue
				}

			case evt, ok = <-fw.w.Event: // ファイル監視イベント
				if !ok {
					return
				}
			case <-CreateTimer:
				changes.NotifyRotated() //IN_CREATEからタイムアウトしたら強制rotate
				return
			case <-ctx.Done():
				return
			}

			switch {
			case evt.Mask&inotify.IN_CLOSE_WRITE != 0:
				if mask&inotify.IN_CREATE != 0 {
					changes.NotifyRotated()
					return
				}
			case evt.Mask&inotify.IN_MODIFY != 0:
				changes.NotifyModified()
			}
		}
	}()

	return changes
}
