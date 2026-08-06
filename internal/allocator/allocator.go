package allocator

import (
	"context"

	"github.com/cenkalti/rain/v2/internal/metainfo"
	"github.com/cenkalti/rain/v2/internal/storage"
)

// Allocator allocates files on the disk.
type Allocator struct {
	Files       []File
	HasExisting bool
	HasMissing  bool
	Error       error

	closeC chan struct{}
	doneC  chan struct{}
}

// File on the disk.
type File struct {
	Storage storage.File
	Name    string
	Padding bool
}

// Progress about the allocation.
type Progress struct {
	AllocatedSize int64
}

// New returns a new Allocator.
func New() *Allocator {
	return &Allocator{
		closeC: make(chan struct{}),
		doneC:  make(chan struct{}),
	}
}

// Close the Allocator.
func (a *Allocator) Close() {
	close(a.closeC)
	<-a.doneC
}

// Run the Allocator.
func (a *Allocator) Run(info *metainfo.Info, sto storage.Storage, progressC chan Progress, resultC chan *Allocator) {
	a.run(info, sto, nil, progressC, resultC)
}

// RunWithPrepare runs prepare in the allocator worker before opening files.
// Closing the allocator cancels preparation and waits for all opened files to
// close, just like cancellation during ordinary allocation.
func (a *Allocator) RunWithPrepare(info *metainfo.Info, sto storage.Storage, prepare func(context.Context) error, progressC chan Progress, resultC chan *Allocator) {
	a.run(info, sto, prepare, progressC, resultC)
}

func (a *Allocator) run(info *metainfo.Info, sto storage.Storage, prepare func(context.Context) error, progressC chan Progress, resultC chan *Allocator) {
	defer close(a.doneC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-a.closeC:
			cancel()
		case <-ctx.Done():
		}
	}()

	defer func() {
		if a.Error != nil || a.closed() {
			for _, f := range a.Files {
				if f.Storage != nil {
					f.Storage.Close()
				}
			}
		}
		select {
		case resultC <- a:
		case <-a.closeC:
		}
	}()
	if prepare != nil {
		a.Error = prepare(ctx)
		if a.Error != nil {
			return
		}
	}

	var allocatedSize int64
	a.Files = make([]File, len(info.Files))
	for i, f := range info.Files {
		if a.closed() {
			a.Error = context.Canceled
			return
		}
		var sf storage.File
		var exists bool
		if f.Padding {
			sf = storage.NewPaddingFile(f.Length)
		} else {
			sf, exists, a.Error = sto.Open(f.Path, f.Length)
			if a.Error != nil {
				return
			}
			if exists {
				a.HasExisting = true
			} else {
				a.HasMissing = true
			}
		}
		a.Files[i] = File{Storage: sf, Name: f.Path, Padding: f.Padding}
		allocatedSize += f.Length
		if !a.sendProgress(progressC, allocatedSize) {
			a.Error = context.Canceled
			return
		}
	}
}

func (a *Allocator) sendProgress(progressC chan Progress, size int64) bool {
	select {
	case progressC <- Progress{AllocatedSize: size}:
		return true
	case <-a.closeC:
		return false
	}
}

func (a *Allocator) closed() bool {
	select {
	case <-a.closeC:
		return true
	default:
		return false
	}
}
