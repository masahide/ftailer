package watch

import "log"

type FileChanges struct {
	Modified chan int // Channel to get notified of modifications
	/*
		Truncated chan bool // Channel to get notified of truncations
		Deleted   chan bool // Channel to get notified of deletions/renames
	*/
}

const (
	chanSize = 1000

	None = iota
	Rotated
	Modified
)

func NewFileChanges() *FileChanges {
	return &FileChanges{
		Modified: make(chan int, chanSize),
	}
}

func (fc *FileChanges) NotifyRotated() {
	fc.Modified <- Rotated

}
func (fc *FileChanges) NotifyModified() {
	fc.Modified <- Modified
}

/*
func (fc *FileChanges) NotifyTruncated() {
	sendOnlyIfEmpty(fc.Truncated)
}

func (fc *FileChanges) NotifyDeleted() {
	sendOnlyIfEmpty(fc.Deleted)
}

func (fc *FileChanges) NotifyClosed() {
	sendOnlyIfEmpty(fc.Rotated)
}
*/

func (fc *FileChanges) Close() {
	log.Printf("Close FileChanges.")
	close(fc.Modified)
	/*
		close(fc.Truncated)
		close(fc.Deleted)
	*/
}

/*
// sendOnlyIfEmpty sends on a bool channel only if the channel has no
// backlog to be read by other goroutines. This concurrency pattern
// can be used to notify other goroutines if and only if they are
// looking for it (i.e., subsequent notifications can be compressed
// into one).
func sendOnlyIfEmpty(ch chan bool) {
	select {
	case ch <- true:
	default:
	}
}
*/
