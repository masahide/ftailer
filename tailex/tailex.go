package tailex

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/masahide/tail"
)

var ErrNoSuchFile = errors.New("No such file.")

type Config struct {
	// logrotate log
	Path string

	// Cronolog
	PathFmt      string        // cronologなどのpathに日付が入る場合
	Time         time.Time     // start日時
	RotatePeriod time.Duration // ログローテーション間隔
	Delay        time.Duration // 切り替えwait

	tail.Config
}

type FileInfo struct {
	Path     string
	CreateAt time.Time
}

type TailEx struct {
	Config
	Lines     chan *tail.Line
	TimeSlice time.Time // 現在のファイルの time slice
	FilePath  string
	FileInfo  chan FileInfo
	old       bool
	done      chan struct{}
	updateAt  time.Time
	offset    int64

	tail *tail.Tail
}

// time.Truncateを 1 day(24*time.Hour)を指定された場合にtimezoneを考慮するように
// see: http://qiita.com/umisama/items/b50df4888665fc36346e
func Truncate(t time.Time, d time.Duration) time.Time {
	if d == 24*time.Hour {
		return t.Truncate(time.Hour).Add(-time.Duration(t.Hour()) * time.Hour)
	}
	return t.Truncate(d)
}

func TailFile(config Config) *TailEx {
	c := &TailEx{
		Config:    config,
		TimeSlice: Truncate(config.Time, config.RotatePeriod),
		Lines:     make(chan *tail.Line),
		done:      make(chan struct{}),
		FileInfo:  make(chan FileInfo),
	}

	go c.tailFileSyncLoop()
	return c
}

func (c *TailEx) tailFileSyncLoop() {
	for {
		if err := c.newOpen(); err != nil {
			log.Printf("tailFileSyncLoop newOpen err:%s", err)
			return
		}

		c.tailFileSync()

		err := c.tail.Stop() //  古い方を止める
		if err != nil {
			log.Printf("TailEx.tail.Stop err:%s", err)
			c.Stop()
			return
		}
		c.tail = nil
		log.Printf("TailEx.tail.Stop %s", c.TimeSlice)
		c.TimeSlice = c.TimeSlice.Add(c.RotatePeriod)
	}
}

func (c *TailEx) GlobSearchLoop() (string, error) {
	firstFlag := true
	for {
		globPath := Time2Path(c.PathFmt, c.TimeSlice)
		s, err := GlobSearch(globPath)
		if err == nil {
			return s, nil
		} else if err != ErrNoSuchFile {
			return "", err
		}
		if firstFlag {
			log.Printf("%s:GlobSearch s:'%s', %s", err, globPath, c.PathFmt)
			firstFlag = false //TODO: test
		}
		select {
		case <-time.After(1 * time.Second):
		case <-c.done:
			// キャンセル処理
			return "", ErrCancel
		}
		if Truncate(time.Now(), c.RotatePeriod).Sub(c.TimeSlice) > 0 {
			c.TimeSlice = c.TimeSlice.Add(c.RotatePeriod)
		}
	}
}

func (c *TailEx) tailFile() error {
	var err error
	if c.PathFmt != "" {
		c.FilePath, err = c.GlobSearchLoop()
	} else {
		c.FilePath = c.Path
	}
	log.Printf("GlobSearch: FilePath %s", c.FilePath)
	t, err := tail.TailFile(c.FilePath, c.Config.Config)
	if err != nil {
		return err
	}
	c.tail = t
	return err
}

func (c *TailEx) Tell() (offset int64, err error) {
	if c.tail == nil {
		return c.offset, nil
	}
	offset, err = c.tail.Tell()
	return
}

// Stop stops the tailing activity.
func (c *TailEx) Stop() error {
	if c.tail != nil {
		if err := c.tail.Stop(); err != nil {
			return err
		}
		c.tail = nil
	}
	close(c.Lines)
	return nil
}

var ErrCancel = errors.New("Cancel")

func (c *TailEx) tailFileSync() error {
	var n <-chan time.Time
	if c.PathFmt != "" {
		next := c.TimeSlice.Add(c.RotatePeriod)
		nextwait := next.Sub(time.Now())
		c.old = nextwait <= 0
		if c.old {
			nextwait = 0
		}
		log.Printf("set timer nextwait:%v, TimeSlice:%v, old:%v", nextwait, c.TimeSlice, c.old) //TODO: test
		n = time.After(nextwait + c.Delay)
	}
	for {
		select {
		case <-c.done:
			// キャンセル処理
			return ErrCancel
		case l := <-c.tail.Lines:
			//log.Printf("l:%v,%s", l.Time, l.Text) //TODO:test
			c.updateAt = time.Now()
			if c.old {
				l.Time = c.TimeSlice.Add(c.RotatePeriod - 1*time.Second)
			}
			c.Lines <- l
		case createAt := <-c.tail.OpenTime:
			log.Printf("createAt:%v", createAt) //TODO:test
			c.FileInfo <- FileInfo{Path: c.FilePath, CreateAt: createAt}
		case <-n: // cronolog のファイル更新
			if c.old && time.Now().Sub(c.updateAt) < c.Delay {
				n = time.After(c.Delay)
				continue
			}
			return nil
		}
	}
}
func (c *TailEx) newOpen() error {
	err := c.tailFile() // 新しいファイルを開く
	if err != nil {
		log.Printf("TailEx.tailFile file:%s, err:%s", c.FilePath, err)
		c.Stop()
		return err
	}
	log.Printf("Tail Open file %s", c.FilePath)
	fi, err := os.Stat(c.FilePath)
	if err != nil {
		log.Printf("TailEx os.Stat file:%s, err:%s", c.FilePath, err)
		c.Stop()
		return err
	}
	c.FileInfo <- FileInfo{Path: c.FilePath, CreateAt: fi.ModTime()}
	return nil
}

// pathFmtのフォーマット文字列をアンダースコアに置換
func Path2Name(p string) string {
	const escapes = "/\\?*:|\"<>[]% "
	for _, c := range escapes {
		p = strings.Replace(p, string(c), "_", -1)
	}
	return p
}

// Pathの日付フォーマットに日付を適用
// %N は日付ではなくN日前の数値
func Time2Path(p string, t time.Time) string {
	p = strings.Replace(p, "%Y", fmt.Sprintf("%04d", t.Year()), -1)
	p = strings.Replace(p, "%y", fmt.Sprintf("%02d", t.Year()%100), -1)
	p = strings.Replace(p, "%m", fmt.Sprintf("%02d", t.Month()), -1)
	p = strings.Replace(p, "%d", fmt.Sprintf("%02d", t.Day()), -1)
	p = strings.Replace(p, "%H", fmt.Sprintf("%02d", t.Hour()), -1)
	p = strings.Replace(p, "%M", fmt.Sprintf("%02d", t.Minute()), -1)
	p = strings.Replace(p, "%S", fmt.Sprintf("%02d", t.Second()), -1)
	if strings.Index(p, "%N") == -1 { // 数値指定がなければ終わる
		return p
	}
	now := time.Now()
	now = now.Truncate(time.Hour).Add(-time.Duration(now.Hour()) * time.Hour)
	num := int((now.Sub(t) / 24).Hours())
	p = strings.Replace(p, "%N", fmt.Sprintf("%d", num), -1)
	return p

}

func GlobSearch(globPath string) (string, error) {
	matches, err := filepath.Glob(globPath)
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", ErrNoSuchFile
	}
	return matches[0], nil
}
