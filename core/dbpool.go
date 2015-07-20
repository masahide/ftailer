package core

import (
	"errors"
	"log"
	"time"
)

type DBpool struct {
	Path    string
	Name    string
	inTime  time.Time
	outTime time.Time
	Period  time.Duration // time.Minute
	dbs     map[time.Time]*DB
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
	defer func() {
		if r := recover(); r != nil {
			db.Close(false)
			db.Delete(recExt)
			log.Fatalf("Recovered in db.GetPositon : %v", r)
		}
	}()
	if p, err = db.GetPositon(); err != nil {
		return nil, p, err
	}
	//log.Printf("opened DB.: %s:%v", r.Name, t) //TODO: test
	r.dbs[t] = db
	//r.autoClose(t)
	return db, p, nil
}

// CreateDB
func (r *DBpool) CreateDB(t time.Time, pos *Position) (*DB, error) {
	if r.inTime.Sub(t) > 0 {
		log.Printf("%s. 'inTime:%s > baseTime:%s'", ErrTimePast, r.inTime, t)
		return nil, ErrTimePast
	}
	db, ok := r.dbs[t]
	if ok { //  存在している
		return db, nil
	}
	db = &DB{Name: r.Name, Path: r.Path, Time: t}
	if err := db.createDB(recExt, pos); err != nil {
		return nil, err
	}
	//log.Printf("DB was created.: %s:%v", r.Name, t) // TODO: test
	r.dbs[t] = db
	r.inTime = t
	//r.autoClose(t)
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
	if r.inTime.Sub(baseTime) > 0 {
		log.Printf("%s. 'inTime:%s > baseTime:%s'", ErrTimePast, r.inTime, baseTime)
		return ErrTimePast
	}
	if r.inTime.Sub(baseTime) < 0 {
		//log.Printf("r.Close(%s)", r.inTime) //TODO: test
		if err := r.Close(r.inTime, true); err != nil {
			return err
		}
	}
	var err error
	db := r.isOpen(baseTime)
	if db == nil {
		if db, err = r.CreateDB(baseTime, pos); err != nil {
			log.Printf("r.createDB(%s) err:%s", baseTime, err)
			return err
		}
	}
	if err = db.put(record, pos); err != nil {
		return err
	}
	return nil
}

// Close
func (r *DBpool) Close(t time.Time, fix bool) error {
	db, ok := r.dbs[t]
	if !ok {
		return nil
	}
	if err := db.Close(fix); err != nil {
		log.Printf("Close err :%s , %s db.DB:%# v", err, t, db)
		return err
	}
	//log.Printf("DBpool: DB was closed. %s:%s", r.Name, t)  //TODO: test
	delete(r.dbs, t)
	return nil
}

// AllClose
func (r *DBpool) AllClose() {
	for k, db := range r.dbs {
		if err := db.Close(false); err != nil {
			log.Printf("Close err:%s", err)
		}
		delete(r.dbs, k)
	}
	r.dbs = nil
}

func (r *DBpool) CloseOldDbs(t time.Time) (int, error) {
	for k, db := range r.dbs {
		elapsed := db.Time.Add(r.Period + delay).Sub(t)
		if elapsed <= 0 {
			log.Printf("Closed the DB files of old time. wait:%v t:%s", elapsed, db.Time)
			if err := db.Close(true); err != nil {
				log.Printf("Close err:%s", err)
				return len(r.dbs), err
			}
			delete(r.dbs, k)
		}
	}
	return len(r.dbs), nil
}

/*
func (r *DBpool) autoClose(t time.Time) {
	wait := t.Add(r.Period + delay).Sub(time.Now())
	if wait <= 0 {
		log.Printf("autoClose: Closed the DB files of old time. wait:%v t:%s", wait, t)
		r.Close(t, true)
		return
	}
}
*/

// Init
// 最終のdbからPositionを読み込み
func (r *DBpool) Init() (pos *Position, err error) {
	r.dbs = make(map[time.Time]*DB, 0)

	// recファイル検索
	if pos, err = r.recPositon(); err != nil {
		return nil, err
	} else if pos != nil {
		return pos, nil
	}

	// fixed fileを検索
	db := &DB{Path: r.Path, Name: r.Name}
	dbfiles, err := FixGlob(db)
	if err != nil || len(dbfiles) == 0 {
		return nil, err
	}
	f := dbfiles[len(dbfiles)-1]
	db.Time = f.Time
	if err = db.Open(FixExt); err != nil {
		log.Printf("not found db: %s", f.Path)
		return
	}
	var p Position
	if p, err = db.GetPositon(); err != nil {
		log.Printf("db:%s db.GetPositon err: %s", f.Path, err)
		return nil, err
	}
	log.Printf("load positon: %v", p)
	return &p, db.Close(false) //確認したfixedファイルは閉じる
}

func (r *DBpool) recPositon() (*Position, error) {
	db := &DB{Path: r.Path, Name: r.Name}
	dbfiles, err := RecGlob(db)
	if err != nil {
		return nil, err
	}
	if len(dbfiles) <= 0 {
		return nil, nil
	}
	var p Position
	for _, f := range dbfiles {

		if _, p, err = r.open(f.Time); err != nil {
			log.Printf("db open(%s) err: %s", f.Path, err)
			return nil, err
		}

		log.Printf("load positon: %v", p)
		//r.autoClose(db.Time)
	}
	return &p, err
}

func (r *DBpool) makeFilePath(t time.Time) string {
	return MakeFilePath(r.Path, r.Name, t)
}
