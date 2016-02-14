package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/masahide/ftailer/core"
)

type Config struct {
	BufDir string
	Name   string
	Period time.Duration // time.Minute
}

var config = Config{
	BufDir: "",
	Name:   "",
	Period: 1 * time.Minute,
}

var Options = &core.FtailDBOptions{
	ReadOnly: true,
	Bin:      true,
}

func main() {

	flag.StringVar(&config.Name, "name", config.Name, "logfile")
	flag.StringVar(&config.BufDir, "bufdir", config.BufDir, "BufDir path")
	flag.Parse()

	db := &core.DB{Path: config.BufDir, Name: config.Name}
	// fixed fileを検索
	if config.BufDir == "" && config.Name == "" && flag.NArg() >= 1 {
		f := flag.Args()[0]
		db, err := core.FtailDBOpen(f, 0660, Options, nil)
		if err != nil {
			log.Printf("err:%s", err)
			return
		}
		log.Printf("open db: %v -------------", f)
		if _, _, err := db.ReadAll(os.Stdout); err != nil {
			log.Printf("readDB err:%s", err)
		}
	} else {
		dbfiles, err := core.FixGlob(db)
		if err != nil {
			log.Printf("find err:%s", err)
			return
		}
		if len(dbfiles) == 0 {
			log.Printf("not such file :%s", config.BufDir)
			return
		}
		for _, f := range dbfiles {
			db.Time = f.Time
			if err = db.Open(core.FixExt, nil); err != nil {
				log.Printf("not found db: %s", f.Path)
				return
			}
			log.Printf("open db: %v -------------", f)
			if _, _, err := db.ReadAll(os.Stdout); err != nil {
				log.Printf("readDB err:%s", err)
			}
			if err := db.Close(false); err != nil {
				log.Printf("db.Close err:%s", err)
			}

		}
	}
}
