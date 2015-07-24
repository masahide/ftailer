package core

import (
	"testing"
	"time"
)

var db = &DB{
	Path: "/path/to",
	Name: "name",
	Time: time.Now(),
}

func TestMakeFileName(t *testing.T) {

	var makeMakeFileNameTest = []struct {
		input  string
		output string
	}{
		//        RFC3339     = "2006-01-02T15:04:05Z07:00"
		{"2015-07-01T02:04:05+09:00", "020405"},
		{"2015-07-01T02:03:05+09:00", "020305"},
		{"2012-04-02T09:03:05Z", "090305"},
	}

	for _, e := range makeMakeFileNameTest {
		var err error
		db.Time, err = time.ParseInLocation(time.RFC3339, e.input, time.Local)
		if err != nil {
			t.Fatalf("time.ParseInLocation err:%s", err)
		}
		output := db.makeFileName()
		if output != e.output {
			t.Errorf("makeFileName(%q) => %q, want %q", e.input, output, e.output)
		}
	}
}

func TestDBMakeFilePath(t *testing.T) {

	var makeMakeFileNameTest = []struct {
		input  string
		output string
	}{
		//        RFC3339     = "2006-01-02T15:04:05Z07:00"
		{"2015-07-01T02:04:05+09:00", "/path/to/name/20150701"},
		{"2015-07-01T02:03:05+09:00", "/path/to/name/20150701"},
		{"2012-04-02T09:03:05Z", "/path/to/name/20120402"},
	}
	for _, e := range makeMakeFileNameTest {
		var err error
		db.Time, err = time.ParseInLocation(time.RFC3339, e.input, time.Local)
		if err != nil {
			t.Fatalf("time.ParseInLocation err:%s", err)
		}
		output := db.makeFilePath()
		if output != e.output {
			t.Errorf("makeFilePath(%q) => %q, want %q", e.input, output, e.output)
		}
	}
}
