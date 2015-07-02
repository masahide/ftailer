package tailex

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/masahide/tail"
)

type Config struct {
	// logrotate log
	Path string

	// Cronolog
	PathFmt      string    // cronologなどのpathに日付が入る場合
	Time         time.Time // start日時
	RotatePeriod time.Duration
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
	tail      *tail.Tail
	old       bool
	done      chan struct{}
	updateAt  time.Time
}

// time.Truncateを 1 day(24*time.Hour)を指定された場合にtimezoneを考慮するように
// see: http://qiita.com/umisama/items/b50df4888665fc36346e
func Truncate(t time.Time, d time.Duration) time.Time {
	if d == 24*time.Hour {
		return t.Truncate(time.Hour).Add(-time.Duration(t.Hour()) * time.Hour)
	}
	return t.Truncate(d)
}

type FTail struct {
}

func TailFile(config Config) (*TailEx, error) {
	c := &TailEx{
		Config:    config,
		TimeSlice: Truncate(config.Time, config.RotatePeriod),
		Lines:     make(chan *tail.Line),
		done:      make(chan struct{}),
		FileInfo:  make(chan FileInfo),
	}

	err := c.tailFile()
	if err != nil {
		return c, err
	}
	go c.tailFileSync()
	return c, err
}

func (c *TailEx) tailFile() error {
	if c.PathFmt != "" {
		c.FilePath = Time2Path(c.PathFmt, c.TimeSlice)
	} else {
		c.FilePath = c.Path
	}
	t, err := tail.TailFile(c.FilePath, c.Config.Config)
	c.tail = t
	return err
}

func (c *TailEx) Tell() (offset int64, err error) {
	return c.tail.Tell()
}

// Stop stops the tailing activity.
func (c *TailEx) Stop() error {
	if err := c.tail.Stop(); err != nil {
		return err
	}
	close(c.done)
	close(c.Lines)
	return nil
}

func (c *TailEx) tailFileSync() {
	var n <-chan time.Time
	if c.PathFmt != "" {
		next := c.TimeSlice.Add(c.RotatePeriod)
		nextwait := time.Since(next)
		c.old = nextwait <= 0
		if c.old {
			nextwait = 0
		}
		n = time.After(nextwait + c.Delay)
	}
	for {
		select {
		case <-c.done:
			// キャンセル処理
			return
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
			c.TimeSlice = c.TimeSlice.Add(c.RotatePeriod)
			nextwait := time.Since(c.TimeSlice)
			c.old = nextwait <= 0
			if c.old {
				nextwait = 0
			}
			if err := c.tail.Stop(); err != nil { //  古い方を止める
				log.Printf("TailEx.tail.Stop err:%s", err)
				c.Stop()
				return
			}
			err := c.tailFile() // 新しいファイルを開く
			if err != nil {
				log.Printf("TailEx.tailFile file:%s, err:%s", c.FilePath, err)
				c.Stop()
				return
			}
			fi, err := os.Stat(c.FilePath)
			if err != nil {
				log.Printf("TailEx os.Stat file:%s, err:%s", c.FilePath, err)
				c.Stop()
				return
			}
			c.FileInfo <- FileInfo{Path: c.FilePath, CreateAt: fi.ModTime()}
			n = time.After(nextwait + c.Delay)
		}
	}
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
	if strings.Index(p, "%N") == -1 { // 数値指定がなければ終わる
		return p
	}
	now := time.Now()
	now = now.Truncate(time.Hour).Add(-time.Duration(now.Hour()) * time.Hour)
	num := int((now.Sub(t) / 24).Hours())
	p = strings.Replace(p, "%N", fmt.Sprintf("%d", num), -1)
	return p

}
