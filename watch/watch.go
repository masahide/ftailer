// Copyright (c) 2013 ActiveState Software Inc. All rights reserved.

package watch

import (
	"os"

	"golang.org/x/net/context"
)

// FileWatcher monitors file-level events.
type FileWatcher interface {
	// BlockUntilExists blocks until the file comes into existence.
	BlockUntilExists(context.Context) error

	// ChangeEvents reports on changes to a file, be it modification,
	// deletion, renames or truncations. Returned FileChanges group of
	// channels will be closed, thus become unusable, after a deletion
	// or truncation event.
	ChangeEvents(context.Context, os.FileInfo) *FileChanges
}

/*
const (
	headerSize    = 4 * 1024
	logrotateTime = 24*time.Hour - 5*time.Minute
)
*/
