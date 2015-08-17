package watch

import (
	"log"
	"sync"

	//"gopkg.in/fsnotify.v0"
	"golang.org/x/exp/inotify"
)

type InotifyTracker struct {
	mux      sync.Mutex
	watchers map[*inotify.Watcher]bool
}

func NewInotifyTracker() *InotifyTracker {
	t := new(InotifyTracker)
	t.watchers = make(map[*inotify.Watcher]bool)
	return t
}

func (t *InotifyTracker) NewWatcher() (*inotify.Watcher, error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	w, err := inotify.NewWatcher()
	if err == nil {
		t.watchers[w] = true
	}
	return w, err
}

func (t *InotifyTracker) CloseWatcher(w *inotify.Watcher) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, ok := t.watchers[w]; ok {
		err = w.Close()
		delete(t.watchers, w)
	}
	return
}

func (t *InotifyTracker) CloseAll() {
	t.mux.Lock()
	defer t.mux.Unlock()
	for w, _ := range t.watchers {
		if err := w.Close(); err != nil {
			log.Printf("Error closing watcher: %v", err)
		}
		delete(t.watchers, w)
	}
}
