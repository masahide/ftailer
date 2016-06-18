package ftail

import (
	"bytes"
	"compress/zlib"
	"hash"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/masahide/ftailer/core"
	"github.com/masahide/ftailer/tail"
	"github.com/masahide/ftailer/tailex"
	"golang.org/x/net/context"
)

//const defaultMaxHeadHashSize = 1024

type Config struct {
	Name            string
	BufDir          string
	Period          time.Duration // 分割保存インターバル
	MaxHeadHashSize int64

	tailex.Config
}

type Ftail struct {
	rec       *core.Recorder
	lastSlice time.Time
	Pos       *core.Position
	Config

	buf      bytes.Buffer
	Writer   io.WriteCloser
	lastTime time.Time
	headHash hash.Hash64
	head     []byte
}

var tailDefaultConfig = tail.Config{
	ReOpen: true,
	Poll:   false,
	//OpenNotify:  true,
	//MaxLineSize:    16 * 1024 * 1024, // 16MB
	NotifyInterval: 1 * time.Second,
}

// ポジション情報がない場合に実ファイルから取得
func (f *Ftail) position(c Config) (pos *core.Position, err error) {
	var fi os.FileInfo
	var filePath string
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
	} else {
		filePath, err = tailex.GlobSearch(c.Path)
		if err == tailex.ErrNoSuchFile {
			log.Printf("ftail position() GlobSearch(%s)  err: %s", c.Path, err)
			return &core.Position{}, nil
		} else if err != nil {
			log.Printf("ftail position() GlobSearch(%s)  err: %s", c.Path, err)
			return nil, err
		}
	}
	if fi, err = os.Stat(filePath); err != nil {
		log.Printf("Start os.Stat('%s')  err: %s,  ", filePath, err)
		return nil, err
	}
	offset := int64(0)
	// 現在のファイルサイズがf.MaxHeadHashSizeより大きいものだけオフセットを現在のサイズにする。
	if (!c.Config.NoSeek) && (fi.Size() > f.MaxHeadHashSize) {
		offset = fi.Size()
	}
	pos = &core.Position{
		Name:     filePath,
		CreateAt: fi.ModTime(),
		Offset:   offset,
	}
	return
}

func Start(ctx context.Context, c Config, w chan bool) error {
	w <- true
	f := &Ftail{
		Config:   c,
		headHash: fnv.New64(),
		head:     []byte{},
	}
	//if f.MaxHeadHashSize == 0 {
	//	f.MaxHeadHashSize = defaultMaxHeadHashSize
	//}
	var err error
	f.rec, err = core.NewRecorder(c.BufDir, c.Name, c.Period)
	if err != nil {
		log.Fatalln("NewRecorder err:", err)
	}
	defer f.rec.AllClose()

	f.Pos = f.rec.Position()
	if f.Pos == nil {
		if f.Pos, err = f.position(c); err != nil {
			log.Fatalln("position err:", err)
		}
	}
	f.Config.Config.Config = tailDefaultConfig
	f.ReOpenDelay = 5 * time.Second
	if f.Delay != 0 {
		f.ReOpenDelay = f.Delay
	}
	log.Printf("f.Pos: %s", f.Pos)

	if f.MaxHeadHashSize != 0 && f.Pos.Name != "" {
		oldhead := f.head
		hash, length, hherr := f.getHeadHash(f.Pos.Name, f.Pos.HashLength)
		if hherr != nil {
			log.Printf("getHeadHash err:%s", hherr)
		} else {
			if f.Pos.HeadHash == hash && f.Pos.HashLength == length { // ポジションファイルのハッシュ値と一致した場合はSeekInfoをセット
				log.Printf("match headHash: %s, head:%s", f.Pos, f.head)
				f.Location = &tail.SeekInfo{Offset: f.Pos.Offset}
			} else {
				log.Printf("not match headHash old: %s, head:%s", f.Pos, oldhead)
				f.Pos.HeadHash = hash
				f.Pos.HashLength = length
				log.Printf("not match headHash new: %s, head:%s", f.Pos, f.head)
			}
		}
	} else {
		posTimeSlise := tailex.Truncate(f.Pos.CreateAt, c.RotatePeriod)
		nowTimeSlise := tailex.Truncate(time.Now(), c.RotatePeriod)
		if nowTimeSlise.Equal(posTimeSlise) { // 読み込んだポジションのcreateAtが現在のtimesliseと同じ場合
			f.Location = &tail.SeekInfo{Offset: f.Pos.Offset}
		}
	}
	t := tailex.NewTailEx(ctx, f.Config.Config, w)
	//var buf bytes.Buffer
	f.buf = bytes.Buffer{}
	/*
		f.Writer, err = zlib.NewWriterLevel(&f.buf, zlib.BestCompression)
		if err != nil {
			log.Fatalln("NewZlibWriter err:", err)
		}
	*/
	<-w
	f.Writer = NopCloser(&f.buf)
	defer func() {
		if err := f.Flush(); err != nil {
			log.Printf("f.Flush err:%s", err)
		}
	}()

	for {
		select {
		case <-ctx.Done(): // キャンセル処理
			return ctx.Err()
		case line, ok := <-t.Lines: // 新しい入力行の取得
			if !ok {
				return err
			}
			err := f.lineNotifyAction(ctx, line, w)
			if err != nil {
				return err
			}
		}

	}

}

// lineのNotifyType別に処理を分岐
func (f *Ftail) lineNotifyAction(ctx context.Context, line *tail.Line, w chan bool) error {
	var err error

	if line.NotifyType == tail.NewLineNotify { // 新しいライン
		return f.Write(line)
	}
	w <- true
	defer func() { <-w }()
	switch line.NotifyType {
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
		if _, cerr := f.rec.CloseOldDbs(line.Time); cerr != nil {
			log.Printf("CloseOldDbs err:%s", cerr)
			return cerr
		}
	case tail.NewFileNotify:
		f.lastTime = line.Time
		f.Pos.Name = line.Filename
		f.Pos.CreateAt = line.OpenTime
		f.Pos.Offset = line.Offset
		maxsize := line.Offset
		if f.MaxHeadHashSize < line.Offset {
			maxsize = f.MaxHeadHashSize
		}
		f.Pos.HeadHash, f.Pos.HashLength, err = f.getHeadHash(f.Pos.Name, maxsize)
		if err != nil {
			log.Printf("getHeadHash err:%s", err)
			return err
		}
		log.Printf("NewFileNotify getHeadHash :%s", f.Pos)
	}
	return nil
}

func (f *Ftail) addHash(line []byte) error {
	restSize := f.MaxHeadHashSize - f.Pos.HashLength
	if int64(len(line)) >= restSize {
		line = line[0:restSize]
	}
	written, err := f.headHash.Write(line)
	if err != nil {
		return err
	}
	f.head = append(f.head, line...)
	f.Pos.HeadHash = strconv.FormatUint(f.headHash.Sum64(), 16)
	f.Pos.HashLength += int64(written)
	log.Printf("addHash name:%s Pos:%s, head:%s", f.Name, f.Pos, string(f.head))
	return nil
}

func (f *Ftail) Write(line *tail.Line) (err error) {
	f.lastTime = line.Time
	f.Pos.Name = line.Filename
	f.Pos.CreateAt = line.OpenTime
	f.Pos.Offset = line.Offset
	if f.Pos.HashLength < f.MaxHeadHashSize {
		if err := f.addHash(line.Text); err != nil {
			return err
		}
	}
	_, err = f.Writer.Write(line.Text)
	return err
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

// NopCloser returns a ReadCloser with a no-op Close method wrapping
// the provided Reader r.
func NopCloser(w io.Writer) io.WriteCloser {
	return nopCloser{w}
}
func (f *Ftail) Flush() error {
	if f.buf.Len() <= 0 {
		return nil
	}
	var b bytes.Buffer
	w, err := zlib.NewWriterLevel(&b, zlib.BestCompression)
	if err != nil {
		return err
	}
	row := core.Row{Time: f.lastTime, Pos: f.Pos}
	row.Text = f.buf.String()
	_, err = io.Copy(w, &f.buf)
	if cerr := w.Close(); cerr != nil {
		log.Printf("zlib close err:%s", cerr)
	}
	if err != nil {
		return err
	}
	if b.Len() < f.buf.Len() {
		row.Bin = b.Bytes()
		row.Text = ""
	}
	//log.Printf("text:'%s',bin:'%x', buf.String:%s", row.Text, row.Bin, f.buf.String())
	defer f.buf.Reset()
	if err = f.rec.Put(row); err != nil {
		log.Printf("Flush %s err:%s", f.Pos.Name, err)
	}
	return err
}

func (f *Ftail) getHeadHash(fname string, getLength int64) (hash string, length int64, err error) {
	f.headHash = fnv.New64()
	f.head = []byte{}
	if f.MaxHeadHashSize == 0 || f.Pos.Name == "" {
		return "", 0, nil
	}
	var readFile *os.File
	readFile, err = os.Open(fname)
	if err != nil {
		return
	}
	defer func() {
		if err := readFile.Close(); err != nil {
			log.Printf("readFile.Close err:%s", err)
		}
	}()
	tee := io.TeeReader(io.LimitReader(readFile, getLength), f.headHash)
	f.head, err = ioutil.ReadAll(tee)
	length = int64(len(f.head))
	//length, err = io.CopyN(f.headHash, readFile, getLength)
	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
		return
	}
	hash = strconv.FormatUint(f.headHash.Sum64(), 16)
	return
}
