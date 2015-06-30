package ftail

import (
	"log"
	"os"
	"time"

	"github.com/masahide/ftailer/core"
	"github.com/masahide/tail"
	"golang.org/x/net/context"
)

type FTailConfig struct {
	Name     string
	Path     string
	BufDir   string
	Interval time.Duration
}

type FTail struct {
}

var tailConfig = tail.Config{
	Follow:      true,
	ReOpen:      true,
	Poll:        true,
	OpenNotify:  true,
	MaxLineSize: 16 * 1024 * 1024, // 16MB
}

func Start(ctx context.Context, c FTailConfig) error {
	t, err := tail.TailFile(c.Path, tailConfig)
	if err != nil {
		log.Fatalln(err)
	}
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
	for {
		select {
		case <-ctx.Done():
			// キャンセル処理
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
