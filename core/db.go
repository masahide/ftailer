package core

import (
	"log"
	"os"
	"path"
	"time"

	"github.com/boltdb/bolt"
)

const (
	recExt = ".rec"
	fixExt = ".fixed"
	delay  = 10 * time.Second
)

type DB struct {
	Path string
	Name string
	Time time.Time
	*bolt.DB
	fix bool
}

func (d *DB) put(record Record, pos *Position) error {
	err := d.Update(func(tx *bolt.Tx) error {
		if err := record.Put(tx, pos); err != nil {
			return err
		}
		return pos.Put(tx)
	})
	return err
}

func (d *DB) GetPositon() (pos Position, err error) {
	err = d.View(func(tx *bolt.Tx) error {
		pos, err = GetPositon(tx)
		return err
	})
	return pos, err
}

func (db *DB) open(ext string) error {
	if db.DB != nil {
		return nil
	}
	filePath := db.makeFilePath()
	os.MkdirAll(filePath, 0755)
	fp := path.Join(filePath, db.makeFileName())
	var err error
	db.DB, err = bolt.Open(fp+ext, 0600, nil)
	if err != nil {
		db.DB = nil
		return err
	}
	return db.DB.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		if err = createRecordBucket(tx); err != nil {
			return err
		}
		if err = createPosBucket(tx); err != nil {
			return err
		}
		return nil
	})
}
func (db *DB) Close(fix bool) error {
	if db.DB == nil {
		return nil
	}
	if fix {
		db.fix = true
	}
	if err := db.DB.Close(); err != nil {
		return err
	}
	db.DB = nil
	if db.fix {
		// mv recExt fixExt
		fileName := path.Join(db.makeFilePath(), db.makeFileName())
		log.Printf("mv %s %s", fileName+recExt, fileName+fixExt) // TODO: test
		if err := os.Rename(fileName+recExt, fileName+fixExt); err != nil {
			return err
		}
	}
	return nil
}

func makeFilePath(filePath, fileName string, t time.Time) string {
	return path.Join(filePath, fileName, t.Format("20060102"))
}
func makeFileName(t time.Time) string {
	return t.Format("150405")
}

func (db *DB) makeFilePath() string {
	return makeFilePath(db.Path, db.Name, db.Time)
}
func (db *DB) makeFileName() string {
	return makeFileName(db.Time)
}

func (r *DBpool) makeFilePath(t time.Time) string {
	return makeFilePath(r.Path, r.Name, t)
}
