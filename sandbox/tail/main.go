package main

import (
	"log"

	"github.com/ActiveState/tail"
)

func main() {
	t, err := tail.TailFile("/tmp/log", tail.Config{Follow: true, ReOpen: true})
	if err != nil {
		log.Panic(err)
	}
	for line := range t.Lines {
		log.Printf(line.Text)
	}
}
