package core

import (
	"fmt"
	"time"
)

const (
	posBucketName = "position"
)

type Position struct {
	Name       string
	CreateAt   time.Time
	Offset     int64
	HeadHash   string
	HashLength int64
}

func (p Position) String() string {
	return fmt.Sprintf("Name:%s, CreateAt:%s, Offset:%d, HeadHash:%s, hashLen:%d", p.Name, p.CreateAt, p.Offset, p.HeadHash, p.HashLength)

}

/*
func createPosBucket(tx *bolt.Tx) error {
	_, err := tx.CreateBucketIfNotExists([]byte(posBucketName))
	return err
}

func (p Position) Put(tx *bolt.Tx) error {
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
	if err = b.Put([]byte("HeadHash"), []byte(p.HeadHash)); err != nil {
		return err
	}
	if err = b.Put([]byte("HashLength"), []byte(strconv.FormatInt(p.HashLength, 16))); err != nil {
		return err
	}
	return nil
}
*/

func GetPositon(db *FtailDB) (Position, error) {
	var p Position
	if db.PosError != nil {
		return p, db.PosError
	}
	return *db.Pos, nil
}
