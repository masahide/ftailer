package main

import (
	"bytes"
	"log"
	"os"
	"testing"
)

func BenchmarkSeek(b *testing.B) {
	defer os.Remove("/tmp/hoge")
	f, _ := os.OpenFile("/tmp/hoge", os.O_RDWR|os.O_CREATE, 0600)
	defer f.Close()
	f.Write(bytes.Repeat([]byte("hogehogehoge"), 1024*1024))
	b.ResetTimer()
	a := int64(0)
	for i := 0; i < b.N; i++ {
		offset, _ := f.Seek(0, os.SEEK_CUR)
		a += offset
	}
	log.Println("seek", a)
}

func BenchmarkStat(b *testing.B) {
	defer os.Remove("/tmp/hoge")
	f, _ := os.OpenFile("/tmp/hoge", os.O_RDWR|os.O_CREATE, 0600)
	defer f.Close()
	f.Write(bytes.Repeat([]byte("hogehogehoge"), 1024*1024))
	b.ResetTimer()
	a := int64(0)
	for i := 0; i < b.N; i++ {
		fi, _ := f.Stat()
		a += fi.Size()
	}
	log.Println("stat", a)
}
