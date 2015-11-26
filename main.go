package main

import (
	"log"
	"sync"
	"time"

	"github.com/masahide/ftailer/in/ftail"
	"github.com/masahide/ftailer/tail"
	"github.com/masahide/ftailer/tailex"
	"golang.org/x/net/context"
)

var registLogConfig = ftail.Config{
	Name:   "regist.log",
	BufDir: "testbuf",
	Period: 1 * time.Minute,
	Config: tailex.Config{
		Path: "/var/log/log_register/regist.log",
		//Path:   "test.log",
		Config: tail.Config{},
	},
}

var accessLogConfig = ftail.Config{
	Name:   "access_log",
	BufDir: "testbuf",
	Period: 1 * time.Minute,
	Config: tailex.Config{
		PathFmt:      "/var/log/httpd/%Y%m%d/kibana-test.access_log",
		RotatePeriod: 24 * time.Hour,
		Time:         time.Now(),
		Delay:        10 * time.Second,
		Config:       tail.Config{},
	},
}
var testLogConfig = ftail.Config{
	Name:   "test_log",
	BufDir: "testbuf",
	Period: 1 * time.Minute,
	Config: tailex.Config{
		PathFmt:      "testlog/%Y%m%d/%H%M.log",
		RotatePeriod: 2 * time.Minute,
		Time:         time.Now(),
		Delay:        5 * time.Second,
		Config:       tail.Config{},
	},
}

var testlogrotateConfig = ftail.Config{
	Name:   "logrotate.log",
	BufDir: "testbuf",
	Period: 5 * time.Minute,
	Config: tailex.Config{
		Path: "testlog/logrotate.log",
		//Path:   "test.log",
		Config: tail.Config{},
	},
}

func main() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	//err := ftail.Start(ctx, accessLogConfig)
	wg := sync.WaitGroup{}
	/*
		wg.Add(1)
		go func() {
			err := ftail.Start(ctx, registLogConfig)
			if err != nil {
				log.Printf("ftail.Start err:%v", err)
			}
		}()
		wg.Add(1)
		go func() {
			err := ftail.Start(ctx, accessLogConfig)
			if err != nil {
				log.Printf("ftail.Start err:%v", err)
			}
		}()
	*/

	wg.Add(1)
	w := make(chan bool, 1)
	go func() {
		err := ftail.Start(ctx, testlogrotateConfig, w)
		if err != nil {
			log.Printf("ftail.Start err:%v", err)
		}
	}()

	wg.Wait()
}
