package main

import (
	"log"
	"sync"
	"time"

	"github.com/masahide/ftailer/in/ftail"
	"github.com/masahide/ftailer/tailex"
	"github.com/masahide/tail"
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
		PathFmt:      "testlog/%Y%m%d/%H.log",
		RotatePeriod: 1 * time.Hour,
		Time:         time.Now(),
		Delay:        10 * time.Second,
		Config:       tail.Config{},
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
	wg.Add(1)
	go func() {
		err := ftail.Start(ctx, testLogConfig)
		if err != nil {
			log.Printf("ftail.Start err:%v", err)
		}
	}()
	wg.Wait()
}
