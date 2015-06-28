package core

import (
	"errors"
	"os"
	"path"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

type DB struct {
	*bolt.DB
	ref int //参照カウンタ
	fix bool
}

func (d *DB) put(record Record, pos Positon) error {
	err := d.Update(func(tx *bolt.Tx) error {
		if err := record.Put(tx, pos); err != nil {
			return err
		}
		return pos.Put(tx)
	})
	return err
}

type DBpool struct {
	Path     string
	Name     string
	inTime   time.Time
	outTime  time.Time
	Interval time.Duration // time.Minute
	dbs      map[time.Time]DB
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

func (r *DBpool) makeFilePath(t time.Time) string {
	return path.Join(r.Path, r.Name, t.Format("20060102"))
}

func (r *DBpool) makeFileName(t time.Time) string {
	//fileName := path.Join(filePath, t.Format("1504")+recExt)
	return t.Format("150405")
}

// Close
func (r *DBpool) Close(t time.Time, fix bool) error {
	if r.dbs == nil {
		return nil
	}
	db, ok := r.dbs[t]
	if !ok {
		return nil
	}
	if fix {
		db.fix = true
	}
	db.ref--
	if db.ref <= 0 {
		if err := db.Close(); err != nil {
			return err
		}
		if db.fix {
			// mv recExt fixExt
			fileName := path.Join(r.makeFilePath(t), r.makeFileName(t))
			if err := os.Rename(fileName+recExt, fileName+fixExt); err != nil {
				return err
			}
		}
		delete(r.dbs, t)
	}
	return nil

}

/*
//  Close and db = nil
func (r *DBpool) Close() error {
	if r.db != nil {
		if err := r.db.Close(); err != nil {
			return err
		}
		r.db = nil
		r.Time = time.Time{}
	}
	return nil
}
*/

func (r *DBpool) autoClose(t time.Time) {
	wait := time.Since(t.Add(r.Interval)) + delay
	if wait <= 0 {
		r.Close(t, true)
		return
	}
	go func(t time.Time, wait time.Duration) {
		time.Sleep(wait)
		r.mu.Lock()
		defer r.mu.Unlock()
		r.Close(t, true)
	}(t, wait)
}

func (r *DBpool) openDb(t time.Time) (DB, error) {
	db := DB{}
	if r.dbs == nil {
		r.dbs = make(map[time.Time]DB, 0)
	}
	if v, ok := r.dbs[t]; ok {
		return v, nil
	}
	filePath := r.makeFilePath(t)
	os.MkdirAll(filePath, 0755)
	fileName := path.Join(filePath, r.makeFileName(t))
	var err error
	db.DB, err = bolt.Open(fileName+recExt, 0600, nil)
	if err != nil {
		return db, err
	}
	db.ref++
	err = db.DB.Update(func(tx *bolt.Tx) error {
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
		return db, err
	}
	r.dbs[t] = db
	return db, nil
}

func (r *DBpool) Put(record Record, pos Positon) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	baseTime := record.Time.Truncate(r.Interval)
	if r.inTime.After(baseTime) {
		return ErrTimePast
	}
	if r.inTime.Before(baseTime) {
		if err := r.Close(baseTime, true); err != nil {
			r.mu.Unlock()
			return err
		}
	}
	db, err := r.openDb(baseTime)
	if err != nil {
		return err
	}
	if err := db.put(record, pos); err != nil {
		return err
	}
	r.autoClose(baseTime)
	return nil
}
