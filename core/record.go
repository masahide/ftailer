package core

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/boltdb/bolt"
)

const (
	recordBucketName = "records"
	CompressSize     = 100
	gzipped          = '1'
	plain            = '0'
)

type Record struct {
	Time time.Time
	Data []byte
}

var (
	ErrKeySizeZero = errors.New("Key size is zero.")
	ErrNotFoundKey = errors.New("Not found key.")
)

func makeKey(t time.Time, pos Position, compress byte) []byte {
	return []byte(fmt.Sprintf("%s_%s_%016x_%c", t.Format(time.RFC3339Nano), pos.CreateAt.Format(time.RFC3339), pos.Offset, compress))
}

func (r Record) Put(tx *bolt.Tx, pos Position) error {
	c := byte(plain)
	if len(r.Data) >= CompressSize {
		if err := r.Compress(); err != nil {
			return err
		}
		c = byte(gzipped)
	}
	return tx.Bucket([]byte(recordBucketName)).Put(makeKey(r.Time, pos, c), r.Data)
}

func GetRecord(tx *bolt.Tx, key []byte) (Record, error) {
	r := Record{}
	if len(key) == 0 {
		return r, ErrKeySizeZero
	}
	r.Data = tx.Bucket([]byte(recordBucketName)).Get(key)
	if r.Data == nil {
		return r, ErrNotFoundKey
	}
	if key[len(key)-1] == byte(gzipped) {
		if err := r.Decompress(); err != nil {
			return r, err
		}
	}
	if sv := bytes.Index(key, []byte("_")); sv > 0 {
		r.Time, _ = time.Parse(time.RFC3339Nano, string(key[0:sv]))
	}
	return r, nil
}

func createRecordBucket(tx *bolt.Tx) error {
	_, err := tx.CreateBucketIfNotExists([]byte(recordBucketName))
	return err
}

func (r Record) Compress() error {
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		return err
	}
	w.Write(r.Data)
	w.Close()
	r.Data = buf.Bytes()
	return nil
}
func (r Record) Decompress() error {
	b := bytes.NewReader(r.Data)
	z, err := zlib.NewReader(b)
	if err != nil {
		return err
	}
	r.Data, err = ioutil.ReadAll(z)
	z.Close()
	return err
}
