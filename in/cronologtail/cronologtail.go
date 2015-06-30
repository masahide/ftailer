package cronologtail

import (
	"log"
	"os"
	"time"

	"github.com/masahide/ftailer/core"
	"github.com/masahide/tail"
	"golang.org/x/net/context"
)

type Config struct {
	PathFmt string
	Period  time.Duration
	tail.Config
}

type CronologTail struct {
	tail.Tail
}

func Truncate(t time.Time, d time.Duration) time.Time {
	if d == 24*time.Hour {
		return t.Truncate(time.Hour).Add(-time.Duration(t.Hour()) * time.Hour)
	}
	return t.Truncate(d)
}

type FTail struct {
}

var tailDefaultConfig = tail.Config{
	Follow:      true,
	ReOpen:      true,
	Poll:        true,
	OpenNotify:  true,
	MaxLineSize: 16 * 1024 * 1024, // 16MB
}

func Start(ctx context.Context, c FTailConfig) error {
	rec, err := core.NewRecorder(c.BufDir, c.Name, c.Interval)
	if err != nil {
		log.Fatalln(err)
	}
	pos := rec.Position()
	if pos != nil {
		fi, err := os.Stat(c.Name)
		if err != nil {
			log.Fatalln(err)
		}
		pos = &core.Position{
			Name:     c.Name,
			CreateAt: fi.ModTime(),
			Offset:   0,
		}
	}
	conf := tailDefaultConfig
	conf.Location = &tail.SeekInfo{Offset: pos.Offset}
	t, err := tail.TailFile(c.Path, conf)
	if err != nil {
		log.Fatalln(err)
	}
	for {
		select {
		case <-ctx.Done():
			// キャンセル処理
			rec.AllClose()
			return ctx.Err()
		case pos.CreateAt = <-t.OpenTime:
		case line := <-t.Lines:
			pos.Offset, err = t.Tell()
			if err != nil {
				return err
			}
			err = rec.Put(core.Record{Time: line.Time, Data: []byte(line.Text)}, pos)
			if err != nil {
				return err
			}
		}
	}
}
