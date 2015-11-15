package core

import (
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const (
	recExt = ".rec"
	FixExt = ".fixed"
	delay  = 0 * time.Second
)

type DB struct {
	RealFilePath string
	Path         string
	Name         string
	Time         time.Time

	*FtailDB
	fix bool
}

func (d *DB) GetPositon() (pos Position, err error) {
	if d.PosError != nil {
		return Position{}, d.PosError
	}
	return *d.Pos, nil
}

func (db *DB) createDB(ext string, pos *Position) error {
	if db.FtailDB != nil {
		return nil
	}
	db.RealFilePath = db.MakeRealFilePath(ext)
	os.MkdirAll(filepath.Dir(db.RealFilePath), 0755)
	var err error
	db.FtailDB, err = FtailDBOpen(db.RealFilePath, 0644, nil)
	if err != nil {
		db.FtailDB = nil
		return err
	}
	log.Printf("DB was created. %s", db.RealFilePath)
	return err
}

func (db *DB) Open(ext string) error {
	if db.FtailDB != nil {
		return nil
	}
	db.RealFilePath = db.MakeRealFilePath(ext)
	var err error
	db.FtailDB, err = FtailDBOpen(db.RealFilePath, 0644, nil)
	if err != nil {
		db.FtailDB = nil
	}
	log.Printf("DB was opened.  %s", db.RealFilePath)
	return err
}

func (db *DB) MakeRealFilePath(ext string) string {
	if db.RealFilePath == "" {
		return db.MakeFilefullPath(ext)
	}
	return strings.TrimSuffix(db.RealFilePath, filepath.Ext(db.RealFilePath)) + ext
}

func (db *DB) Close(fix bool) error {
	if db.FtailDB == nil {
		return nil
	}
	if fix {
		db.fix = true
	}
	if err := db.FtailDB.Close(); err != nil {
		return err
	}
	db.FtailDB = nil
	recFilePath := db.MakeRealFilePath(recExt)
	if db.fix {
		// mv recExt FixExt
		fixFilePath := db.MakeRealFilePath(FixExt)
		if err := os.Rename(recFilePath, fixFilePath); err != nil {
			return err
		}
		log.Printf("DB was closed.  %s -> %s", recFilePath, fixFilePath)
	} else {
		log.Printf("DB was closed.  %s", recFilePath)
	}
	return nil
}
func (db *DB) Delete(ext string) error {
	if db.FtailDB != nil {
		return nil
	}
	extFilePath := db.MakeRealFilePath(ext)
	brokenFilePath := db.MakeRealFilePath("broken")
	log.Printf("save mv %s -> %s", extFilePath, brokenFilePath)
	return os.Rename(extFilePath, brokenFilePath)
}

/*
func makeFilePath(filePath, fileName string, t time.Time) string {
	return path.Join(filePath, fileName, t.Format("20060102"))
}
*/

/*
func makeDBFileName(t time.Time) string {
	return t.Format("150405")
}
*/

/*
func (db *DB) makeFilePath() string {
	return path.Join(db.Path, db.Name, db.Time.Format("20060102"))
	//	return makeFilePath(db.Path, db.Name, db.Time)
}
func (db *DB) makeFileName() string {
	return db.Time.Format("150405")
	//return makeDBFileName(db.Time)
}
*/

func (db *DB) MakeFilefullPath(ext string) string {
	return path.Join(db.Path, db.Name, db.Time.Format("20060102"), db.Time.Format("150405")) + ext
}
