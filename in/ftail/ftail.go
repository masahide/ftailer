package ftail

import (
	"log"
	"os"
	"time"

	"github.com/masahide/ftailer/core"
	"github.com/masahide/ftailer/tailex"
	"github.com/masahide/tail"
	"golang.org/x/net/context"
)

type Config struct {
	Name   string
	BufDir string
	Period time.Duration // 分割保存インターバル

	tailex.Config
}

var tailDefaultConfig = tail.Config{
	Follow:      true,
	ReOpen:      true,
	Poll:        false,
	OpenNotify:  true,
	MaxLineSize: 16 * 1024 * 1024, // 16MB
}

func Start(ctx context.Context, c Config) error {
	rec, err := core.NewRecorder(c.BufDir, c.Name, c.Period)
	if err != nil {
		log.Fatalln(err)
	}
	defer rec.AllClose()
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
	conf := tailex.Config{
		Config: tailDefaultConfig,
	}
	conf.Location = &tail.SeekInfo{Offset: pos.Offset}
	t, err := tailex.TailFile(conf)
	if err != nil {
		log.Fatalln(err)
	}
	for {
		select {
		case <-ctx.Done():
			// キャンセル処理
			return ctx.Err()
		case line, ok := <-t.Lines:
			if !ok {
				return nil
			}
			pos.Offset, err = t.Tell()
			if err != nil {
				return err
			}
			err = rec.Put(core.Record{Time: line.Time, Data: []byte(line.Text)}, pos)
			if err != nil {
				return err
			}
		case fi := <-t.FileInfo:
			pos.Offset = 0
			pos.Name = fi.Path
			pos.CreateAt = fi.CreateAt
		}
	}
}
