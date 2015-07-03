package ftail

import (
	"bytes"
	"compress/zlib"
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
		log.Fatalln("NewRecorder err:", err)
	}
	defer rec.AllClose()
	pos := rec.Position()
	if pos == nil {
		filePath := c.Path
		if c.PathFmt != "" {
			timeSlice := tailex.Truncate(time.Now(), c.Period)
			filePath, err = tailex.GlobSearch(tailex.Time2Path(c.PathFmt, timeSlice))
		}
		var fi os.FileInfo
		if err == nil {
			fi, err = os.Stat(filePath)
		}
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
	t := tailex.TailFile(c.Config)
	saveTick := time.Tick(1 * time.Second)
	//var buf bytes.Buffer
	buf, err := NewlineBuf(rec)
	if err != nil {
		log.Fatalln("NewlineBuf err:", err)
	}

	for {
		select {
		case <-ctx.Done():
			// キャンセル処理
			buf.Flush(pos)
			return ctx.Err()
		case <-saveTick:
			pos.Offset, err = t.Tell()
			if err != nil {
				log.Printf("t.Tell err", err)
				return err
			}
			buf.Flush(pos)
			if err != nil {
				return err
			}

		case line, ok := <-t.Lines:
			if !ok {
				return buf.Flush(pos)
			}
			if err = buf.Write(line); err != nil {
				return err
			}
			//err = rec.Flush(core.Record{Time: line.Time, Data: []byte(line.Text)}, pos)
		case closeTime := <-rec.CloseAlert:
			rec.Close(closeTime, true)
			if _, err = rec.CreateDB(tailex.Truncate(time.Now(), c.Period)); err != nil {
				log.Printf("CreateDB err", err)
				return err
			}
		case fi := <-t.FileInfo:
			pos.Offset = 0
			pos.Name = fi.Path
			pos.CreateAt = fi.CreateAt
		}
	}
}

type lineBuf struct {
	buf bytes.Buffer
	*zlib.Writer
	rec      *core.Recorder
	lastTime time.Time
}

func NewlineBuf(rec *core.Recorder) (l *lineBuf, err error) {
	l = &lineBuf{}
	l.rec = rec
	l.Writer, err = zlib.NewWriterLevel(&l.buf, zlib.BestCompression)
	return
}
func (l *lineBuf) Write(line *tail.Line) (err error) {
	l.lastTime = line.Time
	if _, err = l.Writer.Write(line.Text); err != nil {
		return err
	}
	_, err = l.Writer.Write([]byte("\n"))
	return err
}

func (l *lineBuf) Flush(pos *core.Position) error {
	if l.buf.Len() <= 0 {
		return nil
	}
	l.Writer.Close()
	err := l.rec.Put(core.Record{Time: l.lastTime, Data: l.buf.Bytes()}, pos)
	l.buf.Reset()
	l.Reset(&l.buf)
	return err
}
