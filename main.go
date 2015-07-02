package main

import (
	"log"
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
		//Path: "/var/log/log_register/regist.log",
		PathFmt:      "/var/log/httpd/%Y%m%d/kitsune.access_log",
		Time:         time.Now(),
		RotatePeriod: 24 * time.Hour,
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
	err := ftail.Start(ctx, accessLogConfig)
	if err != nil {
		log.Printf("ftail.Start err:%v", err)
	}
}
