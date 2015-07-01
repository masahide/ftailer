package core

import (
	"errors"
	"log"
	"sync"
	"time"
)

type DBpool struct {
	Path    string
	Name    string
	inTime  time.Time
	outTime time.Time
	Period  time.Duration // time.Minute
	dbs     map[time.Time]DB
	mu      sync.RWMutex
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
	if err := db.open(recExt); err != nil {
		return nil, err
	}
	if ok {
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
func (r *DBpool) Put(record Record, pos *Position) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	baseTime := record.Time.Truncate(r.Period)
	log.Printf("inTime:%s baseTime:%s", r.inTime, baseTime) //TODO: test
	if r.inTime.After(baseTime) {
		log.Printf("ErrTimePast inTime:%s baseTime:%s", r.inTime, baseTime) //TODO: test
		return ErrTimePast
	}
	if r.inTime.Before(baseTime) {
		log.Printf("r.Close(%s)", r.inTime) //TODO: test
		if err := r.Close(r.inTime, true); err != nil {
			return err
		}
	}
	var err error
	db := r.isOpen(baseTime)
	if db == nil {
		log.Printf("r.open(%s) ", baseTime) //TODO: test
		if db, err = r.open(baseTime); err != nil {
			log.Printf("r.open(%s) err:%s", baseTime, err)
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
	log.Printf("Close %s db.DB:%v", t, db.DB)
	if db.DB == nil {
		delete(r.dbs, t)
	}
	return nil
}

// Close
func (r *DBpool) AllClose() {
	for k, db := range r.dbs {
		if err := db.Close(false); err != nil {
			log.Printf("Close err:%s", err) //TODO: test
		}
		delete(r.dbs, k)
	}
	r.dbs = nil
}

func (r *DBpool) autoClose(t time.Time) {
	wait := t.Add(r.Period + delay).Sub(time.Now())
	if wait <= 0 {
		log.Printf("autoClose wait :%v t:%s", wait, t) //TODO: test
		r.Close(t, true)
		return
	}
	go func(t time.Time, wait time.Duration) {
		time.Sleep(wait)
		r.mu.Lock()
		defer r.mu.Unlock()
		log.Printf("autoClose t:%s", t) //TODO: test
		r.Close(t, true)
	}(t, wait)
}

// open

func (r *DBpool) Init() (pos *Position, err error) {
	r.dbs = make(map[time.Time]DB, 0)
	db := &DB{Path: r.Path, Name: r.Name}

	// recファイル検索
	dbfiles, err := RecGlob(db)
	if err != nil {
		return
	}
	p := Position{}
	if len(dbfiles) > 0 {
		for _, f := range dbfiles {
			var d *DB
			if d, err = r.open(f.Time); err != nil {
				log.Printf("not found db: %s", f.Path)
				return
			}
			if p, err = d.GetPositon(); err != nil {
				log.Printf("db:%s db.GetPositon err: %s", f.Path, err)
				return nil, err
			}
			log.Printf("load positon: %v", p)
			r.autoClose(db.Time)
		}
		return &p, err
	}

	// fixed fileを検索
	if dbfiles, err = FixGlob(db); err != nil || len(dbfiles) == 0 {
		return
	}
	f := dbfiles[len(dbfiles)-1]
	db.Time = f.Time
	if err = db.open(fixExt); err != nil {
		log.Printf("not found db: %s", f.Path)
		return
	}
	if p, err = db.GetPositon(); err != nil {
		log.Printf("db:%s db.GetPositon err: %s", f.Path, err)
		return nil, err
	}
	log.Printf("load positon: %v", p)
	return &p, db.Close(false)
}
