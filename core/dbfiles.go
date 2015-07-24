package core

import (
	"errors"
	"path"
	"path/filepath"
	"sort"
	"time"
)

type DBFiles struct {
	Time time.Time
	Path string
}

var ErrUnknownPath = errors.New("Unknown path format.")

func RecGlob(db *DB) ([]DBFiles, error) {
	return dbGlob(db, recExt)
}

func FixGlob(db *DB) ([]DBFiles, error) {
	return dbGlob(db, FixExt)
}

/*
func dbGlob(db *DB, ext string) ([]DBFiles, error) {
	var lenExt = len(ext)
	var lenTime = len(pathTimeFormat) + lenExt
	globPath := path.Join(db.Path, db.Name, "*", "*"+ext)
	matches, err := filepath.Glob(globPath)
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	files := make([]DBFiles, len(matches))
	for i, p := range matches {
		if len(p) <= lenTime {
			return nil, ErrUnknownPath
		}
		t, err := time.ParseInLocation(pathTimeFormat, p[len(p)-lenTime:len(p)-lenExt], time.Local)
		if err != nil {
			return nil, err
		}
		files[i] = DBFiles{Time: t, Path: p}
	}
	return files, nil
}
*/

const pathTimeFormat = "20060102/150405"

type dbPathTime struct {
	ext     string
	lenExt  int
	lenTime int
	TimeFmt string
}

func dbGlob(db *DB, ext string) ([]DBFiles, error) {
	f := &dbPathTime{
		ext:     ext,
		lenExt:  len(ext),
		lenTime: len(pathTimeFormat) + len(ext),
		TimeFmt: pathTimeFormat,
	}
	globPath := path.Join(db.Path, db.Name, "*", "*"+ext)
	return Glob(globPath, f.pathToTime)
}

func Glob(globPath string, pathToTime func(string) (time.Time, error)) ([]DBFiles, error) {
	matches, err := filepath.Glob(globPath)
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	files := make([]DBFiles, len(matches))
	for i, p := range matches {
		t, err := pathToTime(p)
		if err != nil {
			return nil, err
		}
		files[i] = DBFiles{Time: t, Path: p}
	}
	return files, nil
}

// "path/to/20060102/150405.拡張子" の時間をパースする
func (f *dbPathTime) pathToTime(p string) (time.Time, error) {
	if len(p) <= f.lenTime {
		return time.Time{}, ErrUnknownPath
	}
	return time.ParseInLocation(f.TimeFmt, p[len(p)-f.lenTime:len(p)-f.lenExt], time.Local)
}
