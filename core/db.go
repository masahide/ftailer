package core

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
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
	// FixExt 閉じられたファイルの拡張子
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

type FtailDB struct {
	bin      bool
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
	Text string    `json:"s,omitempty"`
}

type FtailDBOptions struct {
	ReadOnly bool
	Bin      bool
}

func (db *DB) GetPositon() (pos Position, err error) {
	return GetPositon(db.FtailDB)
}

func (db *DB) Create(ext string, pos *Position) error {
	if db.FtailDB != nil {
		return nil
	}
	db.RealFilePath = db.MakeRealFilePath(ext)
	err := os.MkdirAll(filepath.Dir(db.RealFilePath), 0755)
	if err != nil {
		return err
	}
	db.FtailDB, err = FtailDBOpen(db.RealFilePath, 0644, nil, pos)
	if err != nil {
		db.FtailDB = nil
		return err
	}
	log.Printf("DB was created. %s", db.RealFilePath)
	return err
}

func (db *DB) Open(ext string, pos *Position) error {
	if db.FtailDB != nil {
		return nil
	}
	db.RealFilePath = db.MakeRealFilePath(ext)
	var err error
	db.FtailDB, err = FtailDBOpen(db.RealFilePath, 0644, nil, pos)
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

var DefaultOptions = &FtailDBOptions{
	ReadOnly: false,
	Bin:      true,
}

func FtailDBOpen(path string, mode os.FileMode, options *FtailDBOptions, pos *Position) (*FtailDB, error) {
	db := &FtailDB{path: path, opened: true}
	if options == nil {
		options = DefaultOptions
	}
	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}
	if options.Bin {
		db.bin = true
	}
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.Pos, db.PosError = db.readHeader()
	if db.PosError == io.EOF {
		db.Pos = pos
		if pos == nil {
			return nil, fmt.Errorf("new file: pos is %v, file:%s", pos, path)
		}
		if err := db.writeHeader(pos); err != nil {
			return nil, err
		}
	} else if db.PosError != nil {
		return nil, db.PosError
	}
	if db.readOnly {
		if db.Pos == nil {
			return nil, fmt.Errorf("Unable to get the position. file:%s", path)
		}
		return db, nil
	} else if db.Pos, db.PosError = db.lastPostion(*db.Pos); db.PosError != nil {
		return nil, db.PosError
	}
	return db, nil
}

func (db *FtailDB) writeHeader(pos *Position) error {
	if db.bin {
		data, err := encodeRow(Row{Pos: pos})
		if err != nil {
			return err
		}
		_, err = db.file.Write(data)
		return err
	}
	enc := json.NewEncoder(db.file)
	err := enc.Encode(pos)
	return err
}
func (db *FtailDB) readHeader() (*Position, error) {
	var pos Position
	if db.bin {
		row, err := decodeRow(db.file)
		if err != nil {
			return nil, err
		}
		return row.Pos, nil
	}
	dec := json.NewDecoder(db.file)
	if err := dec.Decode(&pos); err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &pos, nil

}

func (db *FtailDB) lastPostion(pos Position) (*Position, error) {
	_, p, err := db.ReadAll(ioutil.Discard)
	if err != nil {
		return nil, err
	}
	pos.Offset = p.Offset
	pos.HeadHash = p.HeadHash
	pos.HashLength = p.HashLength
	return &pos, nil
}

func (db *FtailDB) Close() error {
	if err := db.file.Close(); err != nil {
		return err
	}
	db.opened = false
	return nil
}

func (db *FtailDB) Put(row Row) error {
	pos := &Position{Offset: row.Pos.Offset, HashLength: row.Pos.HashLength, HeadHash: row.Pos.HeadHash}
	row.Pos = pos
	var err error
	data := []byte{}
	if db.bin {
		if data, err = encodeRow(row); err != nil {
			return err
		}
	} else {
		b, jerr := json.Marshal(row)
		if jerr != nil {
			return jerr
		}
		data = append(b, '\n')
	}
	_, err = db.file.Write(data)
	return err
}

type Decoder interface {
	Decode(e interface{}) error
}

func (db *FtailDB) ReadAll(w io.Writer) (int64, *Position, error) {
	var p = db.Pos
	line := 0
	size := int64(0)
	var dec Decoder
	if !db.bin {
		dec = json.NewDecoder(db.file)
	}
	for {
		row := &Row{}
		line++
		if db.bin {
			var err error
			row, err = decodeRow(db.file)
			if err == io.EOF {
				break
			} else if err != nil {
				return size, nil, &InvalidFtailDBError{Line: line, File: db.path, S: err.Error()}
			}
			row.Pos.Name = db.Pos.Name
			row.Pos.CreateAt = db.Pos.CreateAt
		} else {
			err := dec.Decode(row)
			if err == io.EOF {
				break
			} else if err != nil {
				return size, nil, &InvalidFtailDBError{Line: line, File: db.path, S: err.Error()}
			}
		}
		var sz int64
		if row.Bin != nil {
			r, err := zlib.NewReader(bytes.NewReader(row.Bin))
			if err != nil {
				return size, nil, &InvalidFtailDBError{Line: line, File: db.path, S: err.Error()}
			}
			if sz, err = io.Copy(w, r); err != nil {
				return size, nil, &InvalidFtailDBError{Line: line, File: db.path, S: err.Error()}
			}
		} else {
			if _, err := io.WriteString(w, row.Text); err != nil {
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

func (e *InvalidFtailDBError) Error() string {
	return fmt.Sprintf("Invalid FtailDB Error. file:%s count:%d: err:%v", e.File, e.Line, e.S)
}

/*
type Row struct {
	Time time.Time `json:"t"`
	Pos  *Position `json:"p,omitempty"`
	Bin  []byte    `json:"b,omitempty"`
	Text string    `json:"s,omitempty"`
}
type Position struct {
	Name       string    `json:"n,omitempty"`
	CreateAt   time.Time `json:"ct,omitempty"`
	Offset     int64     `json:"o,omitempty"`
	HashLength int64     `json:"hl,omitempty"`
	HeadHash   string    `json:"h,omitempty"`
}
*/

func encodeRow(r Row) ([]byte, error) {
	var data = []interface{}{
		r.Time.UnixNano(),
		r.Pos.CreateAt.UnixNano(),
		r.Pos.Offset,
		int32(len(r.Bin)),
		int32(len(r.Text)),
		int16(r.Pos.HashLength),
		int16(len(r.Pos.HeadHash)),
		int16(len(r.Pos.Name)),
	}
	buf := &bytes.Buffer{}
	fnvWriter := fnv.New32a()
	w := io.MultiWriter(buf, fnvWriter)
	for _, v := range data {
		err := binary.Write(w, binary.LittleEndian, v)
		if err != nil {
			return nil, fmt.Errorf("encodeRow binary.Write failed1: %s", err)
		}
	}
	if err := binary.Write(w, binary.LittleEndian, fnvWriter.Sum32()); err != nil {
		return nil, fmt.Errorf("encodeRow binary.Write failed checkSum1: %s", err)
	}
	var dataStream = [][]byte{
		r.Bin,
		[]byte(r.Text),
		[]byte(r.Pos.HeadHash),
		[]byte(r.Pos.Name),
	}
	for _, v := range dataStream {
		_, err := w.Write(v)
		if err != nil {
			return nil, fmt.Errorf("encodeRow binary.Write failed2: %s", err)
		}
	}
	if err := binary.Write(buf, binary.LittleEndian, fnvWriter.Sum32()); err != nil {
		return nil, fmt.Errorf("encodeRow binary.Write failed checkSum2: %s", err)
	}
	return buf.Bytes(), nil
}

func decodeRow(f io.Reader) (*Row, error) {
	r := Row{Pos: &Position{}}
	var LenBin, LenText int32
	var hashLength, LenHeadHash, LenName int16
	fnvWriter := fnv.New32a()
	tee := io.TeeReader(f, fnvWriter)

	var times = []*time.Time{
		&r.Time,
		&r.Pos.CreateAt,
	}
	for _, v := range times {
		var t int64
		err := binary.Read(tee, binary.LittleEndian, &t)
		if err == io.EOF {
			return nil, err
		} else if err != nil {
			return nil, fmt.Errorf("decodeRow binary.Read failed1: %v", err)
		}
		if t == (&time.Time{}).UnixNano() {
			*v = time.Time{}
		} else {
			*v = time.Unix(0, t)
		}
	}
	var data = []interface{}{
		&r.Pos.Offset,
		&LenBin,
		&LenText,
		&hashLength,
		&LenHeadHash,
		&LenName,
	}
	for _, v := range data {
		err := binary.Read(tee, binary.LittleEndian, v)
		if err == io.EOF {
			return nil, err
		} else if err != nil {
			return nil, fmt.Errorf("decodeRow binary.Read failed2: %v", err)
		}
	}
	sum := fnvWriter.Sum32()
	var checkSum uint32
	err := binary.Read(tee, binary.LittleEndian, &checkSum)
	if err == io.EOF {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("decodeRow binary.Read failed checkSum1: %s", err)
	}
	if checkSum != sum {
		return nil, fmt.Errorf("decodeRow checksum1 does not match. f:%x sum:%x", checkSum, sum)
	}
	r.Pos.HashLength = int64(hashLength)
	r.Bin = make([]byte, LenBin)
	Text := make([]byte, LenText)
	HeadHash := make([]byte, LenHeadHash)
	Name := make([]byte, LenName)
	var dataStream = [][]byte{r.Bin, Text, HeadHash, Name}
	for _, v := range dataStream {
		if _, terr := tee.Read(v); terr == io.EOF {
			return nil, terr
		} else if terr != nil {
			return nil, fmt.Errorf("decodeRow tee.Read failed: %v", terr)
		}
	}
	sum = fnvWriter.Sum32()
	err = binary.Read(f, binary.LittleEndian, &checkSum)
	if err == io.EOF {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("decodeRow binary.Read failed checkSum2: %s", err)
	}
	if checkSum != sum {
		return nil, fmt.Errorf("decodeRow checksum2 does not match. f:%x sum:%x", checkSum, sum)
	}
	r.Text = string(Text)
	r.Pos.HeadHash = string(HeadHash)
	r.Pos.Name = string(Name)
	if LenBin == 0 {
		r.Bin = nil
	}
	return &r, nil
}
