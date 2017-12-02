package watch

import (
	"context"
	"log"
	"sync"

	"github.com/masahide/fsnotify"
)

type InotifyTracker struct {
	mux      sync.Mutex
	watchers map[*fsnotify.Watcher]bool
}

func NewInotifyTracker() *InotifyTracker {
	t := new(InotifyTracker)
	t.watchers = make(map[*fsnotify.Watcher]bool)
	return t
}

func (t *InotifyTracker) NewWatcher(ctx context.Context) (*fsnotify.Watcher, error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	w, err := fsnotify.NewWatcher(ctx)
	if err == nil {
		t.watchers[w] = true
	}
	return w, err
}

func (t *InotifyTracker) CloseWatcher(ctx context.Context, w *fsnotify.Watcher) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, ok := t.watchers[w]; ok {
		err = w.Close(ctx)
		delete(t.watchers, w)
	}
	return
}

func (t *InotifyTracker) CloseAll(ctx context.Context) {
	t.mux.Lock()
	defer t.mux.Unlock()
	for w, _ := range t.watchers {
		if err := w.Close(ctx); err != nil {
			log.Printf("Error closing watcher: %v", err)
		}
		delete(t.watchers, w)
	}
}
