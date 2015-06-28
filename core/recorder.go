package core

import (
	"errors"
	"os"
	"path"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

type Recorder struct {
	Path     string
	Name     string
	Time     time.Time
	Interval time.Duration // time.Minute
	db       *bolt.DB
	mu       sync.RWMutex
}

const (
	recExt = ".rec"
	fixExt = ".fixed"
	delay  = 10 * time.Second
)

var (
	ErrTimePast = errors.New("Time is past.")
	ErrNotFound = errors.New("Key does not exist.")
)

func (r *Recorder) makeFilePath(t time.Time) string {
	return path.Join(r.Path, r.Name, t.Format("20060102"))
}

func (r *Recorder) makeFileName(t time.Time) string {
	//fileName := path.Join(filePath, t.Format("1504")+recExt)
	return t.Format("150405")
}

func NewRecorder(path, name string, t time.Time, interval time.Duration) *Recorder {
	return &Recorder{
		Interval: interval,
		Path:     path,
		Name:     name,
		Time:     t,
	}
}

// Close and Rename file
func (r *Recorder) fixClose() error {
	if r.db != nil {
		if err := r.Close(); err != nil {
			return err
		}
		// mv recExt fixExt
		fileName := path.Join(r.makeFilePath(r.Time), r.makeFileName(r.Time))
		if err := os.Rename(fileName+recExt, fileName+fixExt); err != nil {
			return err
		}
	}
	return nil
}

//  Close and db = nil
func (r *Recorder) Close() error {
	if r.db != nil {
		if err := r.db.Close(); err != nil {
			return err
		}
		r.db = nil
		r.Time = time.Time{}
	}
	return nil
}

func (r *Recorder) autoClose() {
	wait := time.Since(r.Time.Add(r.Interval)) + delay
	if wait <= 0 {
		r.fixClose()
		return
	}
	go func(t time.Time, wait time.Duration) {
		time.Sleep(wait)
		r.mu.Lock()
		defer r.mu.Unlock()
		if t.Equal(r.Time) {
			r.fixClose()
		}
	}(r.Time, wait)
}

func (r *Recorder) Open(t time.Time) error {
	return r.open(t.Truncate(r.Interval))

}
func (r *Recorder) open(t time.Time) error {
	var err error
	if r.db == nil {
		filePath := r.makeFilePath(t)
		os.MkdirAll(filePath, 0755)
		fileName := path.Join(filePath, r.makeFileName(t))
		if r.db, err = bolt.Open(fileName+recExt, 0600, nil); err != nil {
			return err
		}
		r.Time = t
		err = r.db.Update(func(tx *bolt.Tx) error {
			// Create a bucket.
			if err = createRecordBucket(tx); err != nil {
				return err
			}
			if err = createPosBucket(tx); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Recorder) Put(record Record, pos Positon) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	baseTime := record.Time.Truncate(r.Interval)
	if r.Time.After(baseTime) {
		return ErrTimePast
	}
	if r.Time.Before(baseTime) {
		if err := r.fixClose(); err != nil {
			r.mu.Unlock()
			return err
		}
	}
	if err := r.open(baseTime); err != nil {
		return err
	}
	if err := r.put(record, pos); err != nil {
		return err
	}
	r.autoClose()
	return nil
}

func (r *Recorder) put(record Record, pos Positon) error {
	err := r.db.Update(func(tx *bolt.Tx) error {
		if err := record.Put(tx, pos); err != nil {
			return err
		}
		return pos.Put(tx)
	})
	return err
}
