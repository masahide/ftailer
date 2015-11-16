package core

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
		return err
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

func (db *DB) MakeFilefullPath(ext string) string {
	return path.Join(db.Path, db.Name, db.Time.Format("20060102"), db.Time.Format("150405")) + ext
}

type FtailDB struct {
	path     string
	readOnly bool
	opened   bool
	file     *os.File
	Pos      *Position
	PosError error
}

type Row struct {
	Time time.Time `json:"t"`
	Pos  *Position `json:"p,omitempty"`
	Bin  []byte    `json:"b,omitempty"`
	Text string    `json:"t,omitempty"`
}

type FtailDBOptions struct {
	ReadOnly bool
}

var DefaultOptions = &FtailDBOptions{
	ReadOnly: false,
}

func FtailDBOpen(path string, mode os.FileMode, options *FtailDBOptions) (*FtailDB, error) {
	db := &FtailDB{path: path, opened: true}
	if options == nil {
		options = DefaultOptions
	}
	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.Pos, db.PosError = db.lastPostion()
	if db.PosError != nil {
		return nil, db.PosError
	}

	return db, nil
}

func (db *FtailDB) lastPostion() (*Position, error) {
	_, pos, err := db.ReadAll(ioutil.Discard)
	return pos, err
}

func (db *FtailDB) Close() error {
	if err := db.file.Close(); err != nil {
		return err
	}
	db.opened = false
	return nil
}

func (db *FtailDB) Put(row Row) error {
	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	_, err = db.file.Write(append(b, '\n'))
	return err
}

func (db *FtailDB) ReadAll(w io.Writer) (int64, *Position, error) {
	var p *Position
	line := 0
	size := int64(0)
	dec := json.NewDecoder(db.file)
	for {
		var row Row
		line++
		if err := dec.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			return size, nil, &InvalidFtailDBError{Line: line, File: db.path, S: err.Error()}
		}
		var sz int64
		var err error
		if row.Bin != nil {
			r, err := zlib.NewReader(bytes.NewReader(row.Bin))
			if err != nil {
				return size, nil, &InvalidFtailDBError{Line: line, File: db.path, S: err.Error()}
			}
			if sz, err = io.Copy(w, r); err != nil {
				return size, nil, &InvalidFtailDBError{Line: line, File: db.path, S: err.Error()}
			}
		} else {
			if _, err = io.WriteString(w, row.Text); err != nil {
				return size, nil, &InvalidFtailDBError{Line: line, File: db.path, S: err.Error()}
			}
			sz = int64(len(row.Text))
		}
		size += sz
		p = row.Pos
	}
	return size, p, nil
}

type InvalidFtailDBError struct {
	Line int
	File string
	S    string
}

func (e *InvalidFtailDBError) Error() string { return fmt.Sprintf("%s:%d: %v", e.File, e.Line, e.S) }
