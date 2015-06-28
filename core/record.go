package core

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

const (
	recordBucketName = "records"
)

type Record struct {
	Time time.Time
	Data []byte
}

func makeKey(t time.Time, pos Positon) []byte {
	return []byte(fmt.Sprintf("%s_%s_%016x", t.Format(time.RFC3339Nano), pos.CreateAt.Format(time.RFC3339), pos.Offset))
}

func (r Record) Put(tx *bolt.Tx, pos Positon) error {
	return tx.Bucket([]byte(recordBucketName)).Put(makeKey(r.Time, pos), r.Data)
}

func createRecordBucket(tx *bolt.Tx) error {
	_, err := tx.CreateBucketIfNotExists([]byte(recordBucketName))
	return err
}
