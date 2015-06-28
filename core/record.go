package core

import (
	"fmt"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
)

type Record struct {
	Time time.Time
	Data []byte
}

type Positon struct {
	Name    string
	ModTime time.Time
	Offset  int64
}

func makeKey(t time.Time, pos Positon) []byte {
	return []byte(fmt.Sprintf("%s_%s_%s_%016x", t.Format(time.RFC3339Nano), pos.Name, pos.ModTime.Format(time.RFC3339), pos.Offset))
}

func setPosition(tx *bolt.Tx, p Positon) error {
	var err error
	b := tx.Bucket([]byte(posBucketName))
	if err = b.Put([]byte("Name"), []byte(p.Name)); err != nil {
		return err
	}
	if err = b.Put([]byte("ModTime"), []byte(p.ModTime.Format(time.RFC3339))); err != nil {
		return err
	}
	if err = b.Put([]byte("Offset"), []byte(strconv.FormatInt(p.Offset, 16))); err != nil {
		return err
	}
	return nil
}

func positon(tx *bolt.Tx) (Positon, error) {
	var err error
	var p Positon
	var value []byte
	b := tx.Bucket([]byte(posBucketName))
	if value = b.Get([]byte("Name")); value == nil {
		return p, ErrNotFound
	}
	p.Name = string(value)
	if value = b.Get([]byte("ModTime")); value == nil {
		return p, ErrNotFound
	}
	if p.ModTime, err = time.Parse(time.RFC3339, string(value)); err != nil {
		return p, err
	}
	if value = b.Get([]byte("Offset")); value == nil {
		return p, ErrNotFound
	}
	if p.Offset, err = strconv.ParseInt(string(value), 16, 64); err != nil {
		return p, err
	}
	return p, nil
}
