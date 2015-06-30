package tail

import (
	"log"
	"os"
	"time"

	"github.com/masahide/ftailer/core"
	astail "github.com/masahide/tail"
	"golang.org/x/net/context"
)

type TailConfig struct {
	Name     string
	Path     string
	BufDir   string
	Interval time.Duration
}

type Tail struct {
}

var tailConfig = astail.Config{
	Follow:      true,
	ReOpen:      true,
	Poll:        true,
	MaxLineSize: 16 * 1024 * 1024,
}

func Start(ctx context.Context, c TailConfig) error {
	t, err := astail.TailFile(c.Path, tailConfig)
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
