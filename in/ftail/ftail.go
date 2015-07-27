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
	Pos    *core.Position
	Period time.Duration // 分割保存インターバル

	tailex.Config
}

type Ftail struct {
	rec       *core.Recorder
	lastSlice time.Time
	Config

	buf bytes.Buffer
	*zlib.Writer
	lastTime time.Time
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
	f := &Ftail{Config: c}
	var err error
	f.rec, err = core.NewRecorder(c.BufDir, c.Name, c.Period, c.Pos)
	if err != nil {
		log.Fatalln("NewRecorder err:", err)
	}
	defer f.rec.AllClose()

	f.Pos = f.rec.Position()
	if f.Pos == nil {
		if f.Pos, err = position(c); err != nil {
			log.Fatalln("position err:", err)
		}
	}
	f.Config.Config.Config = tailDefaultConfig
	log.Printf("f.Pos:%v:%v", f.Pos.Name, f.Pos.Offset)
	f.Location = &tail.SeekInfo{Offset: f.Pos.Offset}
	t := tailex.TailFile(ctx, f.Config.Config)
	//var buf bytes.Buffer
	f.buf = bytes.Buffer{}
	f.Writer, err = zlib.NewWriterLevel(&f.buf, zlib.BestCompression)
	if err != nil {
		log.Fatalln("NewZlibWriter err:", err)
	}
	defer f.Flush()

	for {
		select {
		case <-ctx.Done(): // キャンセル処理
			return ctx.Err()
		case line, ok := <-t.Lines: // 新しい入力行の取得
			if !ok {
				return err
			}
			err := f.lineNotifyAction(ctx, line)
			if err != nil {
				return err
			}
		}

	}

}

// lineのNotifyType別に処理を分岐
func (f *Ftail) lineNotifyAction(ctx context.Context, line *tail.Line) error {
	var err error
	switch line.NotifyType {
	case tail.NewLineNotify: // 新しいライン
		err = f.Write(line)
		if err != nil {
			return err
		}
	case tail.TickerNotify, tailex.GlobLoopNotify: // 定期flush処理
		if err := f.Flush(); err != nil {
			return err
		}
		timeSlice := tailex.Truncate(line.Time, f.Period)
		if f.lastSlice.Sub(timeSlice) < 0 {
			// 新しいDBを開く
			if _, err = f.rec.CreateDB(timeSlice, f.Pos); err != nil {
				log.Printf("CreateDB err:%s", err)
				return err
			}
			f.lastSlice = timeSlice
		}
		// 古いDBを閉じる
		if _, err := f.rec.CloseOldDbs(line.Time); err != nil {
			log.Printf("CloseOldDbs err:%s", err)
			return err
		}
	case tail.NewFileNotify:
		f.lastTime = line.Time
		f.Pos.Name = line.Filename
		f.Pos.CreateAt = line.OpenTime
		f.Pos.Offset = line.Offset
	}
	return nil
}

func (f *Ftail) Write(line *tail.Line) (err error) {
	f.lastTime = line.Time
	f.Pos.Name = line.Filename
	f.Pos.CreateAt = line.OpenTime
	f.Pos.Offset = line.Offset
	_, err = f.Writer.Write(line.Text)
	return err
}

func (f *Ftail) Flush() error {
	if f.buf.Len() <= 0 {
		return nil
	}
	f.Writer.Close()
	err := f.rec.Put(core.Record{Time: f.lastTime, Data: f.buf.Bytes()}, f.Pos)
	f.buf.Reset()
	f.Reset(&f.buf)
	if err != nil {
		log.Printf("Flush %s err:%s", f.Pos.Name, err)
	}
	return err
}
