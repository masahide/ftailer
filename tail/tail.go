package tail

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/masahide/ftailer/watch"
)

var (
	// ErrStop  tailを停止
	ErrStop = fmt.Errorf("tail should now stop")
)

const (
	NewLineNotify int = iota
	NewFileNotify
	TickerNotify
)

type Line struct {
	Time       time.Time
	Text       []byte
	Filename   string
	Offset     int64
	OpenTime   time.Time
	Err        error // Error from tail
	NotifyType int
}

// newLine returns a Line with present time.
func newLine(text []byte) *Line {
	return &Line{Text: text, Time: time.Now(), Err: nil}
}

// SeekInfo represents arguments to `os.Seek`
type SeekInfo struct {
	Offset int64
	Whence int // os.SEEK_*
}

// Config is used to specify how a file must be tailed.
type Config struct {
	// File-specifc
	Location    *SeekInfo     // Seek to this location before tailing
	ReOpen      bool          // Reopen recreated files (tail -F)
	ReOpenDelay time.Duration // Reopen Delay

	MustExist      bool // Fail early if the file does not exist
	Poll           bool // Poll for file changes instead of using inotify
	TruncateReOpen bool // copytruncate rotate
	RenameReOpen   bool // rename rotate
	LinesChanSize  int  // Lines channel size

	// Generic IO
	NotifyInterval time.Duration // Notice interval of the elapsed time
}

type Tail struct {
	Filename string
	Lines    chan *Line
	Config

	tracker   *watch.InotifyTracker
	ticker    *time.Ticker
	openTime  time.Time
	WorkLimit chan bool

	watcher watch.FileWatcher
	changes *watch.FileChanges
	Ctx     context.Context
	Cancel  context.CancelFunc

	reader *bufio.Reader
	file   *os.File
	mu     sync.RWMutex
	//lastDelChReceived time.Time // Last delete channel received time
}

// TailFile begins tailing the file. Output stream is made available
// via the `Tail.Lines` channel. To handle errors during tailing,
// invoke the `Wait` or `Err` method after finishing reading from the
// `Lines` channel.
func TailFile(ctx context.Context, filename string, config Config, w chan bool) (*Tail, error) {
	t := &Tail{
		Filename:  filename,
		Lines:     make(chan *Line, config.LinesChanSize),
		Config:    config,
		WorkLimit: w,
	}
	t.Ctx, t.Cancel = context.WithCancel(ctx)

	if t.Poll {
		t.watcher = watch.NewPollingFileWatcher(filename)
	} else {
		t.tracker = watch.NewInotifyTracker()
		w, err := t.tracker.NewWatcher()
		if err != nil {
			return nil, err
		}
		dw, err := t.tracker.NewWatcher()
		if err != nil {
			return nil, err
		}
		dirname := filepath.Dir(t.Filename)
		// Watch for new files to be created in the parent directory.
		err = dw.Watch(dirname)
		if err != nil {
			return nil, err
		}
		log.Printf("start InotifyFileWatcher: %s", filename)
		t.watcher = watch.NewInotifyFileWatcher(filename, dw, w, t.ReOpenDelay)
	}

	if t.MustExist {
		file, err := os.Open(t.Filename)
		if err != nil {
			return nil, err
		}
		t.setFile(file)
	}

	go t.tailFileSync()

	return t, nil
}

func (tail *Tail) getFile() *os.File {
	tail.mu.RLock()
	defer tail.mu.RUnlock()
	return tail.file
}
func (tail *Tail) setFile(file *os.File) {
	tail.mu.Lock()
	defer tail.mu.Unlock()
	tail.file = file
}
func (tail *Tail) fileStat() (os.FileInfo, error) {
	tail.mu.Lock()
	defer tail.mu.Unlock()
	return tail.file.Stat()
}
func (tail *Tail) fileSeek(offset int64, whence int) (ret int64, err error) {
	tail.mu.Lock()
	defer tail.mu.Unlock()
	return tail.file.Seek(offset, whence)
}
func (tail *Tail) fileClose() (err error) {
	tail.mu.Lock()
	defer tail.mu.Unlock()
	return tail.file.Close()
}
func (tail *Tail) readerBuffered() int {
	tail.mu.RLock()
	defer tail.mu.RUnlock()
	return tail.reader.Buffered()
}
func (tail *Tail) readerReset(r io.Reader) {
	tail.mu.Lock()
	defer tail.mu.Unlock()
	tail.reader.Reset(r)
}
func (tail *Tail) setReader(r io.Reader) {
	tail.mu.Lock()
	defer tail.mu.Unlock()
	tail.reader = bufio.NewReader(r)
}
func (tail *Tail) readerReadBytes(delim byte) (line []byte, err error) {
	tail.mu.Lock()
	defer tail.mu.Unlock()
	return tail.reader.ReadBytes(delim)
}

// Tell Return the file's current position, like stdio's ftell().
func (tail *Tail) Tell() (offset int64, err error) {
	return tail.tell()
}

func (tail *Tail) tell() (offset int64, err error) {
	if tail.getFile() == nil {
		return
	}
	offset, err = tail.fileSeek(0, os.SEEK_CUR)
	if err == nil {
		offset -= int64(tail.readerBuffered())
	}
	return
}

func (tail *Tail) close() {
	if tail.getFile() != nil {
		if err := tail.fileClose(); err != nil {
			log.Printf("tail.file.Close err:%s", err)
		}
	}
	tail.setFile(nil)
}

func (tail *Tail) reopen(ctx context.Context) error {
	if tail.getFile() != nil {
		if err := tail.fileClose(); err != nil {
			log.Printf("reopen error. file.Close:%s", err)
			return err
		}
	}
	for {
		file, err := os.Open(tail.Filename)
		if err != nil {
			if os.IsNotExist(err) {
				log.Printf("Waiting for %s to appear...", tail.Filename)
				if err := tail.watcher.BlockUntilExists(ctx); err != nil {
					return fmt.Errorf("Failed to detect creation of %s: %s", tail.Filename, err)
				}
				continue
			}
			return fmt.Errorf("Unable to open file %s: %s", tail.Filename, err)
		}
		tail.setFile(file)
		break
	}
	return nil
}

func (tail *Tail) readLine() ([]byte, error) {
	tail.WorkLimit <- true
	defer func() { <-tail.WorkLimit }()
	line, err := tail.readerReadBytes(byte('\n'))
	if err != nil {
		// Note ReadString "returns the data read before the error" in
		// case of an error, including EOF, so we return it as is. The
		// caller is expected to process it if err is EOF.
		return line, err
	}

	//line = bytes.TrimRight(line, "\n")

	return line, err
}

func (tail *Tail) tailFileSync() {

	//	defer tail.Done()
	defer tail.Cancel()
	defer tail.close()

	tail.ticker = &time.Ticker{}
	if tail.NotifyInterval != 0 {
		tail.ticker = time.NewTicker(tail.NotifyInterval)
	}
	defer tail.ticker.Stop()

	if !tail.MustExist {
		// deferred first open.
		err := tail.reopen(tail.Ctx)
		if err != nil {
			log.Printf("tail.reopen() err: %s", err)
			return
		}
	}

	// Seek to requested location on first open of the file.
	offset := int64(0)
	if tail.Location != nil {
		offset = tail.Location.Offset
		_, err := tail.fileSeek(offset, tail.Location.Whence)
		log.Printf("Seeked %s - %+v\n", tail.Filename, tail.Location)
		if err != nil {
			log.Printf("Seek error on %s: %s", tail.Filename, err)
			return
		}
	}

	tail.openReader()
	select {
	case tail.Lines <- &Line{NotifyType: NewFileNotify, Filename: tail.Filename, Offset: offset, Time: time.Now(), OpenTime: tail.openTime}:
	case <-tail.Ctx.Done():
		return
	}

	// Read line by line.
	for {
		err := tail.readSend()
		if err == io.EOF {

			// When EOF is reached, wait for more data to become
			// available. Wait strategy is based on the `tail.watcher`
			// implementation (inotify or polling).
			err := tail.waitForChanges(tail.Ctx)
			if err != nil {
				if err != ErrStop {
					log.Printf("tail.waitForChanges() err: %s", err)
				}
				return
			}
		} else if err != nil {
			// non-EOF error
			log.Printf("Error reading %s: %s", tail.Filename, err)
			tail.Cancel()
			return
		}

		select {
		case <-tail.Ctx.Done():
			return
		default:
		}
	}
}

func (tail *Tail) readSend() error {
	offset, err := tail.tell()
	if err != nil {
		log.Printf("tail.tell() err: %s", err)
		return err
	}

	line, err := tail.readLine()

	// Process `line` even if err is EOF.
	if err == nil {
		err = tail.sendLine(line)
		if err != nil {
			log.Printf("tail.sendLine() err: %s", err)
			return err
		}
		return nil
	}

	if err != io.EOF {
		return err
	}

	if len(line) != 0 {
		// this has the potential to never return the last line if
		// it's not followed by a newline; seems a fair trade here
		err := tail.seekTo(SeekInfo{Offset: offset, Whence: 0})
		if err != nil {
			log.Printf("tail.seekTo() err: %s", err)
			return err
		}
	}
	return io.EOF

}

// waitForChanges waits until the file has been appended, deleted,
// moved or truncated. When moved or deleted - the file will be
// reopened if ReOpen is true. Truncated files are always reopened.
func (tail *Tail) waitForChanges(ctx context.Context) error {
	if tail.changes == nil {
		st, err := tail.fileStat()
		if err != nil {
			return err
		}
		tail.changes = tail.watcher.ChangeEvents(ctx, st)
	}

	for {

		select {
		case <-tail.ticker.C:
			offset, err := tail.tell()
			if err != nil {
				return err
			}
			select {
			case tail.Lines <- &Line{NotifyType: TickerNotify, Time: time.Now(), Filename: tail.Filename, OpenTime: tail.openTime, Offset: offset}:
			case <-ctx.Done():
				return nil
			}
			continue
		case mode := <-tail.changes.Modified:
			switch mode {
			case watch.None, watch.Modified:
				return nil
			case watch.Rotated:
				if err := tail.readSendAll(); err != nil {
					return err
				}
				if tail.ReOpen {
					log.Printf("Rotated event file %s. Re-opening ...", tail.Filename)
					tail.changes = nil
					if err := tail.reopen(ctx); err != nil {
						return err
					}
					log.Printf("Successfully reopened %s", tail.Filename)
					tail.openReader()
					select {
					case tail.Lines <- &Line{NotifyType: NewFileNotify, Filename: tail.Filename, Offset: 0, Time: time.Now(), OpenTime: tail.openTime}:
					case <-ctx.Done():
					}
					return nil
				}
				tail.changes = nil
				log.Printf("Stopping tail as file no longer exists: %s", tail.Filename)
				return ErrStop
			default:
				log.Printf("tail.changes.Modified: mode:%v", mode)
				if err := tail.readSendAll(); err != nil {
					return err
				}
				return ErrStop
			}
		case <-ctx.Done():
			return ErrStop
		}
	}
}

func (tail *Tail) readSendAll() error {
	for {
		err := tail.readSend()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (tail *Tail) openReader() {
	tail.setReader(tail.getFile())
	fi, err := os.Stat(tail.Filename)
	if err != nil {
		tail.openTime = time.Now()
		return
	}
	tail.openTime = fi.ModTime()
}

func (tail *Tail) seekEnd() error {
	return tail.seekTo(SeekInfo{Offset: 0, Whence: 2})
}

func (tail *Tail) seekTo(pos SeekInfo) error {
	_, err := tail.fileSeek(pos.Offset, pos.Whence)
	if err != nil {
		return fmt.Errorf("Seek error on %s: %s", tail.Filename, err)
	}
	// Reset the read buffer whenever the file is re-seek'ed
	tail.readerReset(tail.getFile())
	return nil
}

// sendLine sends the line(s) to Lines channel, splitting longer lines
// if necessary. Return false if rate limit is reached.
func (tail *Tail) sendLine(line []byte) error {
	now := time.Now()
	offset, err := tail.tell()
	if err != nil {
		//tail.Kill(err)
		return err
	}
	select {
	case tail.Lines <- &Line{NotifyType: NewLineNotify, Text: line, Time: now, Filename: tail.Filename, OpenTime: tail.openTime, Offset: offset}:
	case <-tail.Ctx.Done():
	}
	return nil
}

// Cleanup removes inotify watches added by the tail package. This function is
// meant to be invoked from a process's exit handler. Linux kernel may not
// automatically remove inotify watches after the process exits.
func (tail *Tail) Cleanup() {
	if tail.tracker != nil {
		tail.tracker.CloseAll()
	}
	tail.Cancel()
}
