package core

import "time"

type Recorder struct {
	DBpool
	pos *Position
}

func (r *Recorder) Position() *Position { return r.pos }

func NewRecorder(filePath, name string, period time.Duration, Pos *Position) (*Recorder, error) {
	var err error
	r := &Recorder{
		DBpool: DBpool{
			Period: period,
			Path:   filePath,
			Name:   name,
		},
	}
	r.pos, err = r.Init()
	if err != nil {
		return nil, err
	}
	if r.pos == nil {
		r.pos = Pos
	}
	return r, nil
}
