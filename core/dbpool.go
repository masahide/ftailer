package core

import (
	"errors"
	"sync"
	"time"
)

type DBpool struct {
	Path     string
	Name     string
	inTime   time.Time
	outTime  time.Time
	Interval time.Duration // time.Minute
	dbs      map[time.Time]DB
	mu       sync.RWMutex
}

var (
	ErrTimePast = errors.New("Time is past.")
	ErrNotFound = errors.New("Key does not exist.")
)

// open
func (r *DBpool) open(t time.Time) (*DB, error) {
	db, ok := r.dbs[t]
	if !ok {
		db = DB{
			Name: r.Name,
			Path: r.Path,
			Time: t,
		}
	}
	if err := db.open(); err != nil {
		return nil, err
	}
	if !ok {
		return &db, nil
	}
	r.dbs[t] = db
	r.autoClose(t)
	return &db, nil
}

func (r *DBpool) isOpen(t time.Time) *DB {
	db, ok := r.dbs[t]
	if !ok {
		return nil
	}
	return &db
}

// Put
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
	var err error
	db := r.isOpen(baseTime)
	if db == nil {
		if db, err = r.open(baseTime); err != nil {
			return err
		}
	}
	if err := db.put(record, pos); err != nil {
		return err
	}
	r.inTime = baseTime
	return nil
}

// Close
func (r *DBpool) Close(t time.Time, fix bool) error {
	db, ok := r.dbs[t]
	if !ok {
		return nil
	}
	if err := db.Close(fix); err != nil {
		return err
	}
	if db.DB == nil {
		delete(r.dbs, t)
	}
	return nil
}

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

// open

func (r *DBpool) Init() (pos *Positon, err error) {
	r.dbs = make(map[time.Time]DB, 0)
	db := &DB{Path: r.Path, Name: r.Name}
	dbfiles, err := RecGlob(db)
	for _, f := range dbfiles {
		db.Time = f.Time
		if err = db.open(); err != nil {
			return
		}
		var p Positon
		if p, err = db.GetPositon(); err != nil {
			return nil, err
		}
		pos = &p
		r.autoClose(db.Time)
	}
	return
}
