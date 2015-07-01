package ftail

import (
	"bytes"
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
	if pos == nil {
		filePath := c.Path
		if c.PathFmt != "" {
			timeSlice := tailex.Truncate(time.Now(), c.RotatePeriod)
			filePath = tailex.Time2Path(c.PathFmt, timeSlice)
		}
		fi, err := os.Stat(filePath)
		if err != nil {
			log.Printf("Start os.Stat('%s')  err: %s,  ", filePath, err)
			pos = &core.Position{}
		} else {
			pos = &core.Position{
				Name:     filePath,
				CreateAt: fi.ModTime(),
				Offset:   0,
			}
		}
	}
	c.Config.Config = tailDefaultConfig
	c.Location = &tail.SeekInfo{Offset: pos.Offset}
	t, err := tailex.TailFile(c.Config)
	if err != nil {
		log.Fatalln(err)
	}
	saveTick := time.Tick(1 * time.Second)
	var buf bytes.Buffer
	var lastTime time.Time
	for {
		select {
		case <-ctx.Done():
			// キャンセル処理
			if buf.Len() > 0 {
				err = rec.Put(core.Record{Time: lastTime, Data: buf.Bytes()}, pos)
			}
			return ctx.Err()
		case <-saveTick:
			pos.Offset, err = t.Tell()
			if err != nil {
				return err
			}
			if buf.Len() > 0 {
				err = rec.Put(core.Record{Time: lastTime, Data: buf.Bytes()}, pos)
				if err != nil {
					return err
				}
				buf.Reset()
			}

		case line, ok := <-t.Lines:
			if !ok {
				err = nil
				if buf.Len() > 0 {
					err = rec.Put(core.Record{Time: lastTime, Data: buf.Bytes()}, pos)
				}
				return err
			}
			lastTime = line.Time
			if _, err = buf.Write(line.Text); err != nil {
				return err
			}
			if err = buf.WriteByte(byte('\n')); err != nil {
				return err
			}
			//err = rec.Put(core.Record{Time: line.Time, Data: []byte(line.Text)}, pos)
		case fi := <-t.FileInfo:
			pos.Offset = 0
			pos.Name = fi.Path
			pos.CreateAt = fi.CreateAt
		}
	}
}
