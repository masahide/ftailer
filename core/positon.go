package core

import (
	"strconv"
	"time"

	"github.com/boltdb/bolt"
)

const (
	posBucketName = "positon"
)

type Positon struct {
	Name     string
	CreateAt time.Time
	Offset   int64
}

func createPosBucket(tx *bolt.Tx) error {
	_, err := tx.CreateBucketIfNotExists([]byte(posBucketName))
	return err
}

func (p Positon) Put(tx *bolt.Tx) error {
	var err error
	b := tx.Bucket([]byte(posBucketName))
	if err = b.Put([]byte("Name"), []byte(p.Name)); err != nil {
		return err
	}
	if err = b.Put([]byte("CreateAt"), []byte(p.CreateAt.Format(time.RFC3339))); err != nil {
		return err
	}
	if err = b.Put([]byte("Offset"), []byte(strconv.FormatInt(p.Offset, 16))); err != nil {
		return err
	}
	return nil
}

func GetPositon(tx *bolt.Tx) (Positon, error) {
	var err error
	var p Positon
	var value []byte
	b := tx.Bucket([]byte(posBucketName))
	if value = b.Get([]byte("Name")); value == nil {
		return p, ErrNotFound
	}
	p.Name = string(value)
	if value = b.Get([]byte("CreateAt")); value == nil {
		return p, ErrNotFound
	}
	if p.CreateAt, err = time.Parse(time.RFC3339, string(value)); err != nil {
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
