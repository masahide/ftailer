package main

import (
	"log"
	"time"

	"github.com/masahide/ftailer/in/ftail"
	"github.com/masahide/ftailer/tailex"
	"github.com/masahide/tail"
	"golang.org/x/net/context"
)

func main() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	err := ftail.Start(ctx, ftail.Config{
		Name:   "test.log",
		BufDir: "testbuf",
		Period: 1 * time.Minute,
		Config: tailex.Config{
			Path:   "test.log",
			Config: tail.Config{},
		},
	})
	if err != nil {
		log.Printf("ftail.Start err:%v", err)
	}
}
