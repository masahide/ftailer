package core

import (
	"bytes"
	"testing"
	"time"
)

var db = &DB{
	Path: "/path/to",
	Name: "name",
	Time: time.Now(),
}

func TestDBMakeFilefullPath(t *testing.T) {

	var makeMakeFileNameTest = []struct {
		input  string
		output string
	}{
		//        RFC3339     = "2006-01-02T15:04:05Z07:00"
		{"2015-07-01T02:04:05+09:00", "/path/to/name/20150701/020405ext"},
		{"2015-07-01T02:03:05+09:00", "/path/to/name/20150701/020305ext"},
		{"2012-04-02T09:03:05Z", "/path/to/name/20120402/090305ext"},
	}
	for _, e := range makeMakeFileNameTest {
		var err error
		db.Time, err = time.ParseInLocation(time.RFC3339, e.input, time.Local)
		if err != nil {
			t.Fatalf("time.ParseInLocation err:%s", err)
		}
		output := db.MakeFilefullPath("ext")
		if output != e.output {
			t.Errorf("makeFilePath(%q) => %q, want %q", e.input, output, e.output)
		}
	}
}

func TestEncodeRowDecodeRow(t *testing.T) {
	testDatas := []Row{
		{Pos: &Position{}},
		{Time: time.Now(), Pos: &Position{Name: "hoge", CreateAt: time.Now()}},
	}
	for _, testData := range testDatas {
		data, err := encodeRow(testData)
		if err != nil {
			t.Error(err)
		}
		buf := bytes.NewBuffer(data)
		row, err := decodeRow(buf)
		if err != nil {
			t.Error(err)
		}
		if *row.Pos != *testData.Pos {
			t.Errorf("row.Pos:(%v) != testData.Pos:(%v)", row.Pos, testData.Pos)
		}
	}
}
