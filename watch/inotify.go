package watch

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/masahide/fsnotify"
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
}

func (fw *InotifyFileWatcher) ChangeEvents(ctx context.Context, fi os.FileInfo) *FileChanges {
	changes := NewFileChanges()

	err := fw.w.Watch(fw.Filename)
	if err != nil {
		log.Fatalf("Error watching %v: %v", fw.Filename, err)
	}
	go fw.changeEventsWorker(ctx, changes)
	return changes
}

func (fw *InotifyFileWatcher) removeWatch(ctx context.Context) {
	for i := 0; i < 30; i++ {
		err := fw.w.RemoveWatch(fw.Filename)
		if err == nil {
			return
		}
		switch err := err.(type) {
		default:
			log.Printf("RemoveWatch(%s) err:%s", fw.Filename, err)
			return
		case *os.SyscallError:
			log.Printf("RemoveWatch(%s) SysCallError:%s", fw.Filename, err)
		}
		time.Sleep(5 * time.Second)
	}
}

func (fw *InotifyFileWatcher) changeEventsWorker(ctx context.Context, changes *FileChanges) {
	defer fw.removeWatch(ctx)
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
		case <-ctx.Done():
			return
		case evt, ok = <-fw.dw.Event: // ディレクトリ監視イベント
			if !ok {
				return
			}
			if !evt.IsCreate() { // IsCreate 以外は無視
				continue
			}
			evtName, err := filepath.Abs(evt.Name)
			if err != nil {
				return
			}
			if evtName == fwFilename {
				inCreate = true
				log.Printf("Received IsCreate: %s", fwFilename)
				CreateTimer = time.After(fw.delay)
				continue
			}
		case err, ok := <-fw.dw.Error: // ディレクトリ監視イベント
			if !ok {
				return
			}
			log.Printf("fw.Filename=%s, fw.dw.Error:%s", fw.Filename, err)
		case evt, ok = <-fw.w.Event: // ファイル監視イベント
			if !ok {
				return
			}
		case err, ok := <-fw.w.Error: // ファイル監視イベント
			if !ok {
				return
			}
			log.Printf("fw.Filename=%s, fw.w.Error:%s", fw.Filename, err)
		case <-CreateTimer:
			log.Printf("IsCreate timeout: %s", fwFilename)
			changes.NotifyRotated(ctx) //IsCreateからタイムアウトしたら強制rotate
			return
		}

		switch {
		case evt.IsCloseWrite():
			if inCreate {
				log.Printf("Received IsCreate & IsCloseWrite: %s", fwFilename)
				changes.NotifyRotated(ctx)
				return
			}
		case evt.IsModify():
			changes.NotifyModified(ctx)
		}
	}

}
