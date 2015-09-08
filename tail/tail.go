package tail

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/net/context"

	"github.com/masahide/ftailer/watch"
)

var (
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

// NewLine returns a Line with present time.
func NewLine(text []byte) *Line {
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

	// Generic IO
	Follow         bool          // Continue looking for new lines (tail -f)
	NotifyInterval time.Duration // Notice interval of the elapsed time

	// Logger, when nil, is set to tail.DefaultLogger
	// To disable logging: set field to tail.DiscardingLogger
	Logger *log.Logger
}

type Tail struct {
	Filename string
	Lines    chan *Line
	Config

	file     *os.File
	reader   *bufio.Reader
	tracker  *watch.InotifyTracker
	ticker   *time.Ticker
	openTime time.Time

	watcher watch.FileWatcher
	changes *watch.FileChanges

	lastDelChReceived time.Time // Last delete channel received time
}

var (
	// DefaultLogger is used when Config.Logger == nil
	DefaultLogger = log.New(os.Stderr, "", log.LstdFlags)
	// DiscardingLogger can be used to disable logging output
	DiscardingLogger = log.New(ioutil.Discard, "", 0)
)

// TailFile begins tailing the file. Output stream is made available
// via the `Tail.Lines` channel. To handle errors during tailing,
// invoke the `Wait` or `Err` method after finishing reading from the
// `Lines` channel.
func TailFile(filename string, config Config) (*Tail, error) {
	t := &Tail{
		Filename: filename,
		Lines:    make(chan *Line),
		Config:   config,
	}

	// when Logger was not specified in config, use default logger
	if t.Logger == nil {
		t.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

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
		t.Logger.Printf("start InotifyFileWatcher: %s", filename)
		t.watcher = watch.NewInotifyFileWatcher(filename, dw, w, t.ReOpenDelay)
	}

	if t.MustExist {
		var err error
		t.file, err = os.Open(t.Filename)
		if err != nil {
			return nil, err
		}
	}

	go t.tailFileSync()

	return t, nil
}

// Return the file's current position, like stdio's ftell().
// But this value is not very accurate.
// it may readed one line in the chan(tail.Lines),
// so it may lost one line.
func (tail *Tail) Tell() (offset int64, err error) {
	if tail.file == nil {
		return
	}
	offset, err = tail.file.Seek(0, os.SEEK_CUR)
	if err == nil {
		offset -= int64(tail.reader.Buffered())
	}
	return
}

// Stop stops the tailing activity.
func (tail *Tail) Stop() error {
	//	tail.Kill(nil)
	//	return tail.Wait()
	return nil
}

func (tail *Tail) close() {
	close(tail.Lines)
	if tail.file != nil {
		tail.file.Close()
	}
}

func (tail *Tail) reopen(ctx context.Context) error {
	if tail.file != nil {
		tail.file.Close()
	}
	for {
		var err error
		tail.file, err = os.Open(tail.Filename)
		if err != nil {
			if os.IsNotExist(err) {
				tail.Logger.Printf("Waiting for %s to appear...", tail.Filename)
				if err := tail.watcher.BlockUntilExists(ctx); err != nil {
					return fmt.Errorf("Failed to detect creation of %s: %s", tail.Filename, err)
				}
				continue
			}
			return fmt.Errorf("Unable to open file %s: %s", tail.Filename, err)
		}
		break
	}
	return nil
}

func (tail *Tail) readLine() ([]byte, error) {
	line, err := tail.reader.ReadBytes(byte('\n'))
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
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	ctx, cancel = context.WithCancel(context.Background())

	//	defer tail.Done()
	defer cancel()
	defer tail.close()

	tail.ticker = &time.Ticker{}
	if tail.NotifyInterval != 0 {
		tail.ticker = time.NewTicker(tail.NotifyInterval)
	}
	defer tail.ticker.Stop()

	if !tail.MustExist {
		// deferred first open.
		err := tail.reopen(ctx)
		if err != nil {
			tail.Logger.Printf("tail.reopen() err: %s", err)
			cancel()
			return
		}
	}

	// Seek to requested location on first open of the file.
	offset := int64(0)
	if tail.Location != nil {
		offset = tail.Location.Offset
		_, err := tail.file.Seek(offset, tail.Location.Whence)
		tail.Logger.Printf("Seeked %s - %+v\n", tail.Filename, tail.Location)
		if err != nil {
			tail.Logger.Printf("Seek error on %s: %s", tail.Filename, err)
			cancel()
			return
		}
	}

	tail.openReader()
	tail.Lines <- &Line{NotifyType: NewFileNotify, Filename: tail.Filename, Offset: offset, Time: time.Now(), OpenTime: tail.openTime}

	// Read line by line.
	for {
		// grab the position in case we need to back up in the event of a half-line
		offset, err := tail.Tell()
		if err != nil {
			cancel()
			tail.Logger.Printf("tail.Tell() err: %s", err)
			return
		}

		line, err := tail.readLine()

		// Process `line` even if err is EOF.
		if err == nil {
			err = tail.sendLine(line)
			if err != nil {
				tail.Logger.Printf("tail.sendLine() err: %s", err)
				cancel()
			}
		} else if err == io.EOF {
			if !tail.Follow {
				if len(line) != 0 {
					err = tail.sendLine(line)
					if err != nil {
						tail.Logger.Printf("tail.sendLine() err: %s", err)
						cancel()
					}
				}
				return
			}

			if tail.Follow && len(line) != 0 {
				// this has the potential to never return the last line if
				// it's not followed by a newline; seems a fair trade here
				err := tail.seekTo(SeekInfo{Offset: offset, Whence: 0})
				if err != nil {
					tail.Logger.Printf("tail.seekTo() err: %s", err)
					cancel()
					return
				}
			}

			// When EOF is reached, wait for more data to become
			// available. Wait strategy is based on the `tail.watcher`
			// implementation (inotify or polling).
			err := tail.waitForChanges(ctx)
			if err != nil {
				if err != ErrStop {
					tail.Logger.Printf("tail.waitForChanges() err: %s", err)
					cancel()
				}
				return
			}
		} else {
			// non-EOF error
			tail.Logger.Printf("Error reading %s: %s", tail.Filename, err)
			cancel()
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// waitForChanges waits until the file has been appended, deleted,
// moved or truncated. When moved or deleted - the file will be
// reopened if ReOpen is true. Truncated files are always reopened.
func (tail *Tail) waitForChanges(ctx context.Context) error {
	if tail.changes == nil {
		st, err := tail.file.Stat()
		if err != nil {
			return err
		}
		tail.changes = tail.watcher.ChangeEvents(ctx, st)
	}

	for {

		select {
		case <-tail.ticker.C:
			offset, err := tail.Tell()
			if err != nil {
				return err
			}
			tail.Lines <- &Line{NotifyType: TickerNotify, Time: time.Now(), Filename: tail.Filename, OpenTime: tail.openTime, Offset: offset}
			continue
		case <-tail.changes.Modified:
			return nil
		case rotated := <-tail.changes.Rotated:
			if !rotated {
				return nil
			}
			if tail.ReOpen {
				tail.Logger.Printf("Rotated event file %s. Re-opening ...", tail.Filename)
				tail.changes = nil
				if err := tail.reopen(ctx); err != nil {
					return err
				}
				tail.Logger.Printf("Successfully reopened %s", tail.Filename)
				tail.openReader()
				tail.Lines <- &Line{NotifyType: NewFileNotify, Filename: tail.Filename, Offset: 0, Time: time.Now(), OpenTime: tail.openTime}
				return nil
			} else {
				tail.changes = nil
				tail.Logger.Printf("Stopping tail as file no longer exists: %s", tail.Filename)
				return ErrStop
			}
		case <-ctx.Done():
			return ErrStop
		}
		panic("unreachable")
	}
}

func (tail *Tail) openReader() {
	tail.reader = bufio.NewReader(tail.file)
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
	_, err := tail.file.Seek(pos.Offset, pos.Whence)
	if err != nil {
		return fmt.Errorf("Seek error on %s: %s", tail.Filename, err)
	}
	// Reset the read buffer whenever the file is re-seek'ed
	tail.reader.Reset(tail.file)
	return nil
}

// sendLine sends the line(s) to Lines channel, splitting longer lines
// if necessary. Return false if rate limit is reached.
func (tail *Tail) sendLine(line []byte) error {
	now := time.Now()
	offset, err := tail.Tell()
	if err != nil {
		//tail.Kill(err)
		return err
	}
	tail.Lines <- &Line{NotifyType: NewLineNotify, Text: line, Time: now, Filename: tail.Filename, OpenTime: tail.openTime, Offset: offset}
	return nil
}

// Cleanup removes inotify watches added by the tail package. This function is
// meant to be invoked from a process's exit handler. Linux kernel may not
// automatically remove inotify watches after the process exits.
func (tail *Tail) Cleanup() {
	if tail.tracker != nil {
		tail.tracker.CloseAll()
	}
}
