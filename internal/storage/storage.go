// Package storage contains an interface for reading and writing files in a torrent.
package storage

import (
	"context"
	"io"
)

// Storage is an interface for reading/writing torrent files.
type Storage interface {
	// Open a file. If the file does not exist, it will be created.
	Open(name string, size int64) (f File, exists bool, err error)
	// RootDir is the absolute path of the storage root.
	RootDir() string
}

// File interface for reading/writing torrent data.
type File interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

// Provider is a torrent storage provider.
type Provider interface {
	// GetStorage returns a storage for a torrent.
	GetStorage(torrentID string) (Storage, error)
}

// Manifest describes the complete ordered torrent byte stream before files
// are opened. Storage backends that need one operation for the whole torrent
// may implement Preparer.
type Manifest struct {
	TorrentID   string
	InfoHash    string
	Name        string
	PieceLength int64
	PieceCount  int
	TotalSize   int64
	Files       []ManifestFile
}

type ManifestFile struct {
	Index        int
	Path         string
	Size         int64
	GlobalOffset int64
	Padding      bool
}

type Preparer interface {
	Prepare(context.Context, Manifest) error
}

// PreserveOnRemove is implemented by storage whose content lifecycle is not
// owned by Rain. Removing the Rain session only detaches it.
type PreserveOnRemove interface {
	PreserveOnRemove() bool
}
