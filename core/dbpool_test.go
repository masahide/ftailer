package core

import (
	"testing"
	"time"
)

var makeFilePathTests = []struct {
	input  string
	output string
}{
	//        RFC3339     = "2006-01-02T15:04:05Z07:00"
	{"2015-07-01T02:04:05+09:00", "/path/to/name/20150701"},
	{"2015-07-01T02:03:05+09:00", "/path/to/name/20150701"},
	{"2012-04-02T09:03:05Z", "/path/to/name/20120402"},
}

var dbpool = &DBpool{
	Path:       "/path/to",
	Name:       "name",
	Period:     1 * time.Minute,
	CloseAlert: make(chan time.Time),
}

func TestMakeFilePath(t *testing.T) {

	for _, e := range makeFilePathTests {
		it, err := time.ParseInLocation(time.RFC3339, e.input, time.Local)
		if err != nil {
			t.Fatalf("time.ParseInLocation err:%s", err)
		}
		output := dbpool.makeFilePath(it)
		if output != e.output {
			t.Errorf("makeFilePath(%q) => %q, want %q", e.input, output, e.output)
		}
	}
}
