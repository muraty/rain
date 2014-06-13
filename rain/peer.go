package rain

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/log"
)

// All current implementations use 2^14 (16 kiB), and close connections which request an amount greater than that.
const blockSize = 16 * 1024

// http://www.bittorrent.org/beps/bep_0020.html
var peerIDPrefix = []byte("-RN0001-")

type peerID [20]byte

const bitTorrent10pstrLen = 19

var bitTorrent10pstr = []byte("BitTorrent protocol")

func (r *Rain) servePeerConn(conn net.Conn) {
	defer conn.Close()
	log.Debugln("Serving peer", conn.RemoteAddr())

	// Give a minute for completing handshake.
	err := conn.SetDeadline(time.Now().Add(time.Minute))
	if err != nil {
		return
	}

	var d *transfer

	resultC := make(chan interface{}, 2)
	go readHandShake(conn, resultC)

	// Send handshake as soon as you see info_hash.
	i := <-resultC
	switch res := i.(type) {
	case infoHash:
		// Do not continue if we don't have a torrent with this infoHash.
		r.transfersM.Lock()
		var ok bool
		if d, ok = r.transfers[res]; !ok {
			log.Error("unexpected info_hash")
			r.transfersM.Unlock()
			return
		}
		r.transfersM.Unlock()

		if err = sendHandShake(conn, res, r.peerID); err != nil {
			log.Error(err)
			return
		}
	case error:
		log.Error(res)
		return
	}

	i = <-resultC
	switch res := i.(type) {
	case peerID:
		if res == r.peerID {
			log.Debug("Rejected own connection: server")
			return
		}
		// TODO save peer_id
	case error:
		log.Error(res)
		return
	}

	log.Debugln("servePeerConn: Handshake completed", conn.RemoteAddr())
	p := newPeerConn(conn, d)
	p.readLoop()
}

func (r *Rain) connectToPeerAndServeDownload(p *Peer, d *transfer) {
	log.Debugln("Connecting to peer", p.TCPAddr())

	conn, err := net.DialTCP("tcp4", nil, p.TCPAddr())
	if err != nil {
		log.Error(err)
		return
	}
	defer conn.Close()

	log.Infoln("Connected to peer", conn.RemoteAddr())

	// Give a minute for completing handshake.
	err = conn.SetDeadline(time.Now().Add(time.Minute))
	if err != nil {
		return
	}

	err = sendHandShake(conn, d.torrentFile.InfoHash, r.peerID)
	if err != nil {
		log.Error(err)
		return
	}

	ih, id, err := readHandShakeBlocking(conn)
	if err != nil {
		log.Error(err)
		return
	}
	if *ih != d.torrentFile.InfoHash {
		log.Error("unexpected info_hash")
		return
	}
	if *id == r.peerID {
		log.Debug("Rejected own connection: client")
		return
	}

	log.Debugln("connectToPeer: Handshake completed", conn.RemoteAddr())
	pc := newPeerConn(conn, d)
	pc.readLoop()
}

// Peer message types
const (
	msgChoke = iota
	msgUnchoke
	msgInterested
	msgNotInterested
	msgHave
	msgBitfield
	msgRequest
	msgPiece
	msgCancel
	msgPort
)

var peerMessageTypes = [...]string{
	"choke",
	"unchoke",
	"interested",
	"not interested",
	"have",
	"bitfield",
	"request",
	"piece",
	"cancel",
	"port",
}

type peerConn struct {
	conn     net.Conn
	transfer *transfer
	bitfield BitField // which pieces does remote peer have?

	unchokeM       sync.Mutex    // protects unchokeC
	unchokeC       chan struct{} // will be closed when and "unchoke" message is received
	onceInterested sync.Once     // for sending "interested" message only once

	amChoking      bool // this client is choking the peer
	amInterested   bool // this client is interested in the peer
	peerChoking    bool // peer is choking this client
	peerInterested bool // peer is interested in this client
	// peerRequests   map[uint64]bool      // What remote peer requested
	// ourRequests    map[uint64]time.Time // What we requested, when we requested it
}

func newPeerConn(conn net.Conn, d *transfer) *peerConn {
	div, mod := divMod(d.torrentFile.TotalLength, int64(d.torrentFile.Info.PieceLength))
	if mod != 0 {
		div++
	}
	log.Debugln("Torrent contains", div, "pieces")
	return &peerConn{
		conn:        conn,
		transfer:    d,
		bitfield:    NewBitField(nil, div),
		unchokeC:    make(chan struct{}),
		amChoking:   true,
		peerChoking: true,
	}
}

const connReadTimeout = 3 * time.Minute

// readLoop processes incoming messages after handshake.
func (p *peerConn) readLoop() {
	log.Debugln("Communicating peer", p.conn.RemoteAddr())

	first := true
	buf := make([]byte, blockSize)
	for {
		err := p.conn.SetReadDeadline(time.Now().Add(connReadTimeout))
		if err != nil {
			log.Error(err)
			return
		}

		var length int32
		err = binary.Read(p.conn, binary.BigEndian, &length)
		if err != nil {
			log.Error(err)
			return
		}

		if length == 0 { // keep-alive message
			log.Debug("Received message of type \"keep alive\"")
			continue
		}

		var msgType byte
		err = binary.Read(p.conn, binary.BigEndian, &msgType)
		if err != nil {
			log.Error(err)
			return
		}
		length--
		log.Debugf("Received message of type %q", peerMessageTypes[msgType])

		switch msgType {
		case msgChoke:
			p.peerChoking = true
		case msgUnchoke:
			p.unchokeM.Lock()
			p.peerChoking = false
			close(p.unchokeC)
			p.unchokeC = make(chan struct{})
			p.unchokeM.Unlock()
		case msgInterested:
			p.peerInterested = true
		case msgNotInterested:
			p.peerInterested = false
		case msgHave:
			var i int32
			err = binary.Read(p.conn, binary.BigEndian, &i)
			if err != nil {
				log.Error(err)
				return
			}
			p.bitfield.Set(int64(i))
			log.Debug("Peer ", p.conn.RemoteAddr(), " has piece #", i)
			log.Debugln("new bitfield:", p.bitfield.Hex())

			// TODO goroutine may leak
			go func() { p.transfer.pieces[i].haveC <- p }()
		case msgBitfield:
			if !first {
				log.Error("bitfield can only be sent after handshake")
				return
			}

			if int64(length) != int64(len(p.bitfield.Bytes())) {
				log.Error("invalid bitfield length")
				return
			}

			_, err = io.LimitReader(p.conn, int64(length)).Read(buf)
			if err != nil {
				log.Error(err)
				return
			}

			p.bitfield = NewBitField(buf, p.bitfield.Len())
			log.Debugln("Received bitfield:", p.bitfield.Hex())

			for i := int64(0); i < p.bitfield.Len(); i++ {
				if p.bitfield.Test(i) {
					// TODO goroutine may leak
					go func(i int64) { p.transfer.pieces[i].haveC <- p }(i)
				}
			}
		case msgRequest:
		case msgPiece:
			msg := &peerPieceMessage{ID: msgPiece}
			err = binary.Read(p.conn, binary.BigEndian, &msg.Index)
			if err != nil {
				log.Error(err)
				return
			}
			err = binary.Read(p.conn, binary.BigEndian, &msg.Begin)
			if err != nil {
				log.Error(err)
				return
			}
			if msg.Begin%blockSize != 0 {
				log.Error("unexpected piece offset")
				return
			}
			length -= 8
			if length != p.transfer.pieces[msg.Index].blocks[msg.Begin/blockSize].length {
				log.Error("unexpected block size")
				return
			}
			msg.Block = make([]byte, length)
			_, err = io.LimitReader(p.conn, int64(length)).Read(msg.Block)
			if err != nil {
				log.Error(err)
				return
			}
			p.transfer.pieces[msg.Index].pieceC <- msg
		case msgCancel:
		case msgPort:
		default:
			log.Debugf("Unknown message type: %d", msgType)
		}

		first = false
	}
}

// beInterested sends "interested" message to peer (once) and
// returns a channel that will be closed when an "unchoke" message is received.
func (p *peerConn) beInterested() (unchokeC chan struct{}, err error) {
	p.unchokeM.Lock()
	defer p.unchokeM.Unlock()

	unchokeC = p.unchokeC

	if !p.peerChoking {
		return
	}

	p.onceInterested.Do(func() { err = p.sendMessage(msgInterested) })
	return
}

func (p *peerConn) sendMessage(msgType byte) error {
	var msg = struct {
		Length      int32
		MessageType byte
	}{1, msgType}
	log.Debugf("Sending message: %q", peerMessageTypes[msgType])
	return binary.Write(p.conn, binary.BigEndian, msg)
}

type peerRequestMessage struct {
	ID                   byte
	Index, Begin, Length int32
}

func newPeerRequestMessage(index, length int32) *peerRequestMessage {
	return &peerRequestMessage{msgRequest, index, index * blockSize, length}
}

func (m *peerRequestMessage) send(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, m)
}

type peerPieceMessage struct {
	ID           byte
	Index, Begin int32
	Block        []byte
}
