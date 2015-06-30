package main

import (
	"log"
	"time"
)

const pathTimeFormat = "20060102/150405"
const LenRecExt = len(recExt)
const LenRecTime = len(pathTimeFormat) + LenRecExt
const (
	recExt = ".rec"
	fixExt = ".fixed"
	delay  = 10 * time.Second
)

func main() {
	p := "/hogefuga/hoge/20150630/102000.rec"

	if len(p) <= LenRecTime {
		log.Fatalln("dame")
	}
	jst := time.FixedZone("Asia/Tokyo", 9*60*60)
	t, err := time.ParseInLocation(pathTimeFormat, p[len(p)-LenRecTime:len(p)-LenRecExt], jst)
	//t, err := time.Parse(pathTimeFormat, p[len(p)-LenRecTime:len(p)-LenRecExt])
	log.Printf("t: %v, p: %v, err: %v", t, p, err)
	log.Printf("t: %v", time.Now())
	log.Printf("now-t: %v", time.Now().Sub(t))
	log.Printf("now.Location: %v", time.Now().Location())
	l, _ := time.ParseInLocation(pathTimeFormat, p[len(p)-LenRecTime:len(p)-LenRecExt], time.Local)
	log.Printf("l: %v", l)
	log.Printf("now-l: %v", time.Now().Sub(l))

}
