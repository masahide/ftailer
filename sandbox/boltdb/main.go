// boltdb test

package main

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

const BucketName = "logfile"

func main() {
	for i := 0; i < 5000; i++ {
		mainloop()
		log.Println("close db")
		time.Sleep(10 * time.Second)
	}

}

func mainloop() {
	db, err := bolt.Open("dbfile", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("open db")
	defer db.Close()

	lineNum := 0
	db.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		tx.CreateBucket([]byte(BucketName))
		return nil
	})

	var wg sync.WaitGroup

	go func() {
		// write
		wg.Add(1)
		defer wg.Done()
		for n := 0; n < 10; n++ {
			i := 0
			for ; i < 5000; i++ {

				s := bytes.Repeat([]byte("hoge"), 4000)
				lineNum++
				key := []byte(fmt.Sprintf("%s_%010d", time.Now().Format(time.RFC3339), lineNum))
				// Start a write transaction.
				db.Update(func(tx *bolt.Tx) error {
					tx.Bucket([]byte(BucketName)).Put(key, s)
					return nil
				})
			}
			fmt.Printf("wirte line:%v\n", i)
			time.Sleep(1 * time.Second)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	go func() {
		// read & delete
		wg.Add(1)
		defer wg.Done()
		for n := 0; n < 10; n++ {

			l := 0
			prefix := []byte(fmt.Sprintf("%s", time.Now().Format(time.RFC3339)))
			err := db.View(func(tx *bolt.Tx) error {
				c := tx.Bucket([]byte(BucketName)).Cursor()
				for k, _ := c.First(); k != nil && bytes.Compare(k[0:bytes.Index(k, []byte("_"))], prefix) < 0; k, _ = c.Next() {
					l++
				}
				return nil
			})
			if err != nil {
				log.Printf("err:%s", err)
				return
			}
			fmt.Printf("read line:%v\n", l)
			l = 0
			err = db.Update(func(tx *bolt.Tx) error {
				c := tx.Bucket([]byte(BucketName)).Cursor()
				for k, _ := c.First(); k != nil && bytes.Compare(k[0:bytes.Index(k, []byte("_"))], prefix) < 0; k, _ = c.Next() {
					if err := c.Delete(); err != nil {
						log.Printf("err:%s key=%s,", err, k)
						return err
					}
					l++
				}
				return nil
			})
			if err != nil {
				log.Printf("err:%s", err)
				return
			}
			fmt.Printf("delete line:%v\n", l)
			time.Sleep(1 * time.Second)
		}
	}()

	wg.Wait()

}
