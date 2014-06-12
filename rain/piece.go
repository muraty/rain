package rain

import (
	"crypto/sha1"
	"errors"
	"os"
	"sync"
)

type piece struct {
	index  int64
	sha1   [sha1.Size]byte
	length int64 // last piece may not be full
	// blocks []*block
	targets []*pieceTarget
}

type pieceTarget struct {
	file           *os.File
	offset, length int64
}

func (p *piece) download(wg *sync.WaitGroup) {
	var wgBlock sync.WaitGroup
	// wgBlock.Add(len(p.blocks))
	// for _, p := range p.blocks {
	// 	go p.download(&wgBlock)
	// }

	wgBlock.Wait()
	// All blocks in the piece have been downloaded.

	// TODO hash check

	// TODO pass bytes
	// _, err := p.write(nil)
	// if err != nil {
	// 	panic(err)
	// }

	wg.Done()
}

func (p *piece) write(b []byte) (n int, err error) {
	if int64(len(b)) != p.length {
		err = errors.New("invalid piece length")
		return
	}
	var m int
	for _, t := range p.targets {
		m, err = t.file.WriteAt(b[n:t.length], t.offset)
		n += m
		if err != nil {
			return
		}
	}
	return
}
