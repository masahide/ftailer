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
	Follow: true,
	ReOpen: true,
	Poll:   false,
	//OpenNotify:  true,
	MaxLineSize:    16 * 1024 * 1024, // 16MB
	NotifyInterval: 1 * time.Second,
}

// ポジション情報がない場合に実ファイルから取得
func position(c Config) (pos *core.Position, err error) {
	var fi os.FileInfo
	filePath := c.Path
	if c.PathFmt != "" { // cronolog
		timeSlice := tailex.Truncate(c.Config.Time, c.RotatePeriod)
		searchPath := tailex.Time2Path(c.PathFmt, timeSlice)
		filePath, err = tailex.GlobSearch(searchPath)
		if err == tailex.ErrNoSuchFile {
			log.Printf("ftail position() GlobSearch(%s)  err: %s", searchPath, err)
			return &core.Position{}, nil
		} else if err != nil {
			log.Printf("ftail position() GlobSearch(%s)  err: %s", searchPath, err)
			return nil, err
		}
	}
	if fi, err = os.Stat(filePath); err != nil {
		log.Printf("Start os.Stat('%s')  err: %s,  ", filePath, err)
		return nil, err
	}
	pos = &core.Position{
		Name:     filePath,
		CreateAt: fi.ModTime(),
		Offset:   0,
	}
	return
}

func Start(ctx context.Context, c Config) error {
	rec, err := core.NewRecorder(c.BufDir, c.Name, c.Period)
	var buf *lineBuf
	if err != nil {
		log.Fatalln("NewRecorder err:", err)
	}
	defer rec.AllClose()
	pos := rec.Position()
	if pos == nil {
		if pos, err = position(c); err != nil {
			log.Fatalln("position err:", err)
		}
	}
	c.Config.Config = tailDefaultConfig
	c.Location = &tail.SeekInfo{Offset: pos.Offset}
	t := tailex.TailFile(ctx, c.Config)
	//var buf bytes.Buffer
	buf, err = NewlineBuf(rec)
	defer buf.Flush(pos)

	if err != nil {
		log.Fatalln("NewlineBuf err:", err)
	}

	for {
		select {

		// キャンセル処理
		case <-ctx.Done():
			return ctx.Err()
		// 新しい入力行の取得
		case line, ok := <-t.Lines:
			if !ok {
				return err
			}

			if len(line.Text) == 0 { // Ticker
				// db Flush
				pos.Offset = line.Offset
				pos.Name = line.Filename
				pos.CreateAt = line.OpenTime
				if err := buf.Flush(pos); err != nil {
					return err
				}
				// 古いDBを閉じる
				openNum, err := rec.CloseOldDbs(line.Time)
				if err != nil {
					log.Printf("CloseOldDbs err", err)
					return err
				}
				if openNum == 0 { // 開いているDBが0になったら新しいDBを作成
					timeSlice := tailex.Truncate(time.Now(), c.Period)
					_, err = rec.CreateDB(timeSlice, pos)
					if err != nil {
						log.Printf("CreateDB err", err)
						return err
					}
				}
				continue
			}

			err = buf.Write(line)
			if err != nil {
				return err
			}
			/*
				pos.Offset, err = t.Tell()
				if err != nil {
					log.Printf("t.Tell err", err)
					return err
				}
			*/
			/*
				// 新規入力ファイルの情報保存
				case fi := <-t.FileInfo:
					pos.Offset = 0
					pos.Name = fi.Path
					pos.CreateAt = fi.CreateAt
			*/
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
	_, err = l.Writer.Write(line.Text)
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
	if err != nil {
		log.Printf("Flush %s err:%s", pos.Name, err)
	}
	return err
}
