package core

import (
	"errors"
	"log"
	"time"
)

type DBpool struct {
	Path       string
	Name       string
	inTime     time.Time
	outTime    time.Time
	Period     time.Duration // time.Minute
	dbs        map[time.Time]*DB
	CloseAlert chan time.Time
}

var (
	ErrTimePast = errors.New("Time is past.")
	ErrNotFound = errors.New("Key does not exist.")
)

// open
func (r *DBpool) open(t time.Time) (*DB, Position, error) {
	var ok bool
	var p Position
	var err error
	db, ok := r.dbs[t]
	if ok { // オープン済み
		if p, err = db.GetPositon(); err != nil {
			return nil, p, err
		}
		return db, p, nil
	}
	db = &DB{Name: r.Name, Path: r.Path, Time: t}
	if err = db.Open(recExt); err != nil {
		return nil, p, err
	}
	if p, err = db.GetPositon(); err != nil {
		return nil, p, err
	}
	log.Printf("openDB:%v", t)
	r.dbs[t] = db
	r.autoClose(t)
	return db, p, nil
}

// open
func (r *DBpool) CreateDB(t time.Time) (*DB, error) {
	db, ok := r.dbs[t]
	if ok { //  存在している
		return db, nil
	}
	db = &DB{Name: r.Name, Path: r.Path, Time: t}
	if err := db.createDB(recExt); err != nil {
		return nil, err
	}
	log.Printf("createDB:%v", t)
	r.dbs[t] = db
	r.autoClose(t)
	return db, nil
}

func (r *DBpool) isOpen(t time.Time) *DB {
	db, ok := r.dbs[t]
	if !ok {
		return nil
	}
	return db
}

// Put
func (r *DBpool) Put(record Record, pos *Position) error {
	baseTime := record.Time.Truncate(r.Period)
	//log.Printf("inTime:%s baseTime:%s", r.inTime, baseTime) //TODO: test
	if r.inTime.After(baseTime) {
		log.Printf("ErrTimePast inTime:%s baseTime:%s", r.inTime, baseTime) //TODO: test
		return ErrTimePast
	}
	if r.inTime.Before(baseTime) {
		//log.Printf("r.Close(%s)", r.inTime) //TODO: test
		if err := r.Close(r.inTime, true); err != nil {
			return err
		}
	}
	var err error
	db := r.isOpen(baseTime)
	if db == nil {
		//log.Printf("r.open(%s) ", baseTime) //TODO: test
		if db, err = r.CreateDB(baseTime); err != nil {
			log.Printf("r.createDB(%s) err:%s", baseTime, err)
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
	log.Printf("DBpool Close %s db:%# v", t, db)
	if err := db.Close(fix); err != nil {
		log.Printf("Close err :%s , %s db.DB:%# v", err, t, db)
		return err
	}
	delete(r.dbs, t)
	return nil
}

// Close
func (r *DBpool) AllClose() {
	for k, db := range r.dbs {
		if err := db.Close(false); err != nil {
			log.Printf("Close err:%s", err)
		}
		delete(r.dbs, k)
	}
	r.dbs = nil
}

func (r *DBpool) autoClose(t time.Time) {
	wait := t.Add(r.Period + delay).Sub(time.Now())
	if wait <= 0 {
		log.Printf("autoClose wait :%v t:%s", wait, t)
		r.Close(t, true)
		return
	}
	go func(t time.Time, wait time.Duration) {
		time.Sleep(wait)
		r.CloseAlert <- t
		//log.Printf("autoClose t:%s", t) //TODO: test
		//r.Close(t, true)
	}(t, wait)
}

// open

func (r *DBpool) Init() (pos *Position, err error) {
	r.CloseAlert = make(chan time.Time)
	r.dbs = make(map[time.Time]*DB, 0)
	db := &DB{Path: r.Path, Name: r.Name}

	// recファイル検索
	dbfiles, err := RecGlob(db)
	if err != nil {
		return
	}
	p := Position{}
	if len(dbfiles) > 0 {
		for _, f := range dbfiles {
			if _, p, err = r.open(f.Time); err != nil {
				log.Printf("db open(%s) err: %s", f.Path, err)
				return
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
	if err = db.Open(FixExt); err != nil {
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
