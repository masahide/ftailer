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
	"golang.org/x/net/context"
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

func TailFile(ctx context.Context, config Config) *TailEx {
	c := &TailEx{
		Config:    config,
		TimeSlice: Truncate(config.Time, config.RotatePeriod),
		Lines:     make(chan *tail.Line),
		FileInfo:  make(chan FileInfo),
	}
	log.Printf("init config.Time:%s -> TimeSlice:%s", config.Time, Truncate(config.Time, config.RotatePeriod)) //TODO: test

	go c.tailFileSyncLoop(ctx)
	return c
}

func (c *TailEx) tailFileSyncLoop(ctx context.Context) {
	for {
		// ファイルを開く
		if err := c.newOpen(ctx); err != nil {
			if err != context.Canceled {
				log.Printf("tailFileSyncLoop newOpen err:%s", err)
			}
			return
		}

		//
		tailFileSyncErr := c.tailFileSync(ctx)

		log.Printf("TailEx tail.Stop() %s:%s", c.FilePath, c.TimeSlice)
		err := c.tail.Stop() //  古い方を止める
		if err != nil {
			log.Printf("TailEx.tail.Stop err:%s", err)
			c.Stop()
			return
		}
		//log.Printf("TailEx end tail.Stop %s:%s", c.Path, c.TimeSlice)
		c.tail.Cleanup() //  古い方をcleanup
		//log.Printf("TailEx end tail.cleanup %s:%s", c.Path, c.TimeSlice)
		c.tail = nil

		if tailFileSyncErr != nil {
			return
		}
		c.TimeSlice = c.TimeSlice.Add(c.RotatePeriod)
	}
}

// Glob検索で見つかるまで 1*time.Secondでpolling
func (c *TailEx) GlobSearchLoop(ctx context.Context) (string, error) {
	firstFlag := true
	for {
		globPath := Time2Path(c.PathFmt, c.TimeSlice)
		s, err := GlobSearch(globPath)
		if err == nil {
			return s, nil // 見つかった
		} else if err != ErrNoSuchFile {
			return "", err // その他のエラー
		}
		if firstFlag {
			log.Printf("%s:GlobSearch s:'%s', %s", err, globPath, c.PathFmt)
			firstFlag = false
		}
		select {
		case <-ctx.Done():
			// キャンセル処理
			return "", ctx.Err()
		case <-time.After(1 * time.Second):
		}
		// TimeSliceが過去なら進める
		if Truncate(time.Now(), c.RotatePeriod).Sub(c.TimeSlice) > 0 {
			next := c.TimeSlice.Add(c.RotatePeriod)
			log.Printf("GlobSearchLoop %s: add TimeSlice:%s -> %v", c.PathFmt, c.TimeSlice, next)
			c.TimeSlice = next
		}
	}
}

func (c *TailEx) tailFile(ctx context.Context) error {
	var err error
	if c.PathFmt != "" {
		c.FilePath, err = c.GlobSearchLoop(ctx)
		if err != nil {
			return err
		}
		c.Config.Config.ReOpen = false
	} else {
		c.FilePath = c.Path
	}
	log.Printf("Start tail.TailFile(%s)", c.FilePath) //TODO: test
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
		log.Printf("tail.Stop() Path:%s", c.Path)
		if err := c.tail.Stop(); err != nil {
			return err
		}
		c.tail = nil
	}
	close(c.Lines)
	return nil
}

func (c *TailEx) tailFileSync(ctx context.Context) error {
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
		case <-ctx.Done():
			// キャンセル処理
			//log.Printf("tailFileSync ctx Done. %s:%s, %v", c.FilePath, c.TimeSlice, ctx.Err()) //TODO: test
			return ctx.Err()
		case l := <-c.tail.Lines:
			//log.Printf("l:%v,%s", l.Time, l.Text) //TODO:test
			c.updateAt = time.Now()
			if c.old {
				l.Time = c.TimeSlice.Add(c.RotatePeriod - 1*time.Second)
			}
			c.Lines <- l
		case createAt := <-c.tail.OpenTime:
			fi := FileInfo{Path: c.FilePath, CreateAt: createAt}
			log.Printf("Open FileInfo: Path:%s, CreateAt:%s", fi.Path, fi.CreateAt)
			c.FileInfo <- fi
		case <-n: // cronolog のファイル更新
			if c.old && time.Now().Sub(c.updateAt) < c.Delay {
				log.Printf("set time.After:%v, c.updateAt:%v, old:%v", c.Delay, c.updateAt, c.old) //TODO: test
				n = time.After(c.Delay)
				continue
			}
			return nil
		}
	}
}
func (c *TailEx) newOpen(ctx context.Context) error {
	err := c.tailFile(ctx) // 新しいファイルを開く
	if err != nil {
		if err != context.Canceled {
			log.Printf("TailEx.tailFile file:%s, err:%s", c.FilePath, err)
		}
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
