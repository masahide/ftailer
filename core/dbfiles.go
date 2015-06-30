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

const pathTimeFormat = "20060102/150405"

func RecGlob(db *DB) ([]DBFiles, error) {
	return dbGlob(db, recExt)
}

func FixGlob(db *DB) ([]DBFiles, error) {
	return dbGlob(db, fixExt)
}

func dbGlob(db *DB, ext string) ([]DBFiles, error) {
	var lenExt = len(ext)
	var lenTime = len(pathTimeFormat) + lenExt
	matches, err := filepath.Glob(path.Join(db.Path, "*", "*"+ext))
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
