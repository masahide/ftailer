package core

import "time"

type Recorder struct {
	DBpool
	pos *Position
}

func (r *Recorder) Position() *Position { return r.pos }

func NewRecorder(filePath, name string, interval time.Duration) (*Recorder, error) {
	var err error
	r := &Recorder{
		DBpool: DBpool{
			Interval: interval,
			Path:     filePath,
			Name:     name,
		},
	}
	r.pos, err = r.Init()
	if err != nil {
		return nil, err
	}
	return r, nil
}
