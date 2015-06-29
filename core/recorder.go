package core

import "time"

type Recorder struct {
	DBpool
}

func NewRecorder(filePath, name string, interval time.Duration) *Recorder {
	return &Recorder{
		DBpool: DBpool{
			Interval: interval,
			Path:     filePath,
			Name:     name,
		},
	}
}
