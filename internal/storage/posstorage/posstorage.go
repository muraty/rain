package posstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/rain/v2/internal/storage"
)

type Provider struct {
	controllerURL string
	client        *http.Client
}

const maxPOSRangeSize = 128 << 20

var retryDelays = []time.Duration{
	0,
	100 * time.Millisecond,
	300 * time.Millisecond,
	time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	15 * time.Second,
}

func NewProvider(controllerURL string) (*Provider, error) {
	controllerURL = strings.TrimRight(controllerURL, "/")
	parsed, err := url.Parse(controllerURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("invalid POS controller URL")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 32
	return &Provider{
		controllerURL: controllerURL,
		client:        &http.Client{Transport: transport, Timeout: 5 * time.Minute},
	}, nil
}

func (p *Provider) GetStorage(torrentID string) (storage.Storage, error) {
	if torrentID == "" {
		return nil, fmt.Errorf("empty torrent id")
	}
	return &Storage{
		controllerURL: p.controllerURL,
		torrentID:     torrentID,
		client:        p.client,
		files:         make(map[string]fileMetadata),
	}, nil
}

type Storage struct {
	// prepareMu serializes Prepare calls so concurrent callers cannot both
	// register the download; mu alone is released around the HTTP request.
	prepareMu     sync.Mutex
	mu            sync.RWMutex
	controllerURL string
	torrentID     string
	downloadID    int64
	storeURL      string
	existing      bool
	prepared      bool
	files         map[string]fileMetadata
	client        *http.Client
}

type createRequest struct {
	RainTorrentID string           `json:"rain_torrent_id"`
	InfoHash      string           `json:"info_hash"`
	TotalSize     int64            `json:"total_size"`
	Metadata      downloadMetadata `json:"metadata"`
}

type createResponse struct {
	ID            int64            `json:"id"`
	RainTorrentID string           `json:"rain_torrent_id"`
	InfoHash      string           `json:"info_hash"`
	TotalSize     int64            `json:"total_size"`
	StoreURL      string           `json:"store_url"`
	Metadata      downloadMetadata `json:"metadata"`
}

type downloadMetadata struct {
	Name        string         `json:"name"`
	PieceLength int64          `json:"piece_length"`
	PieceCount  int            `json:"piece_count"`
	Files       []fileMetadata `json:"files"`
}

type fileMetadata struct {
	Index        int    `json:"index"`
	FileID       int64  `json:"file_id"`
	Path         string `json:"path"`
	Size         int64  `json:"size"`
	GlobalOffset int64  `json:"global_offset"`
	Padding      bool   `json:"padding"`
}

func (s *Storage) Prepare(ctx context.Context, manifest storage.Manifest) error {
	s.prepareMu.Lock()
	defer s.prepareMu.Unlock()
	s.mu.Lock()
	if s.prepared {
		// Rain can close and reopen one torrent without recreating its Storage.
		// After the first allocation the registered POS files are existing
		// storage and must be verified if Rain discarded its bitfield.
		s.existing = true
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	request := createRequest{
		RainTorrentID: manifest.TorrentID,
		InfoHash:      manifest.InfoHash,
		TotalSize:     manifest.TotalSize,
		Metadata: downloadMetadata{
			Name: manifest.Name, PieceLength: manifest.PieceLength, PieceCount: manifest.PieceCount,
			Files: make([]fileMetadata, len(manifest.Files)),
		},
	}
	for i, file := range manifest.Files {
		request.Metadata.Files[i] = fileMetadata{
			Index: file.Index, Path: file.Path, Size: file.Size,
			GlobalOffset: file.GlobalOffset, Padding: file.Padding,
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	resp, err := doRequest(ctx, s.client, http.MethodPost, s.controllerURL+"/v2/rain/downloads", "application/json", body)
	if err != nil {
		return fmt.Errorf("registering POS rain download: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return fmt.Errorf("registering POS rain download: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(message)))
	}
	var result createResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decoding POS rain download: %w", err)
	}
	if result.ID <= 0 || result.StoreURL == "" {
		return fmt.Errorf("POS returned incomplete rain download identity")
	}
	if err := validateCreateResponse(request, result); err != nil {
		return fmt.Errorf("validating POS rain download: %w", err)
	}
	files := make(map[string]fileMetadata)
	for _, file := range result.Metadata.Files {
		if !file.Padding {
			files[file.Path] = file
		}
	}
	s.mu.Lock()
	s.downloadID = result.ID
	s.storeURL = strings.TrimRight(result.StoreURL, "/")
	s.existing = resp.StatusCode == http.StatusOK
	s.files = files
	s.prepared = true
	s.mu.Unlock()
	return nil
}

func validateCreateResponse(request createRequest, result createResponse) error {
	storeURL, err := url.Parse(result.StoreURL)
	if err != nil || (storeURL.Scheme != "http" && storeURL.Scheme != "https") || storeURL.Host == "" {
		return fmt.Errorf("invalid store_url")
	}
	if result.RainTorrentID != request.RainTorrentID ||
		!strings.EqualFold(result.InfoHash, request.InfoHash) ||
		result.TotalSize != request.TotalSize {
		return fmt.Errorf("download identity changed")
	}
	if result.Metadata.Name != request.Metadata.Name ||
		result.Metadata.PieceLength != request.Metadata.PieceLength ||
		result.Metadata.PieceCount != request.Metadata.PieceCount ||
		len(result.Metadata.Files) != len(request.Metadata.Files) {
		return fmt.Errorf("manifest metadata changed")
	}
	for i := range request.Metadata.Files {
		want := request.Metadata.Files[i]
		got := result.Metadata.Files[i]
		if got.Index != want.Index || got.Path != want.Path || got.Size != want.Size ||
			got.GlobalOffset != want.GlobalOffset || got.Padding != want.Padding {
			return fmt.Errorf("file %d metadata changed", i)
		}
		if !got.Padding && got.FileID <= 0 {
			return fmt.Errorf("file %d has no POS file id", i)
		}
	}
	return nil
}

func (s *Storage) Open(name string, size int64) (storage.File, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.prepared {
		return nil, false, fmt.Errorf("POS storage opened before manifest registration")
	}
	metadata, ok := s.files[name]
	if !ok {
		return nil, false, fmt.Errorf("file %q is not in POS manifest", name)
	}
	if metadata.Size != size {
		return nil, false, fmt.Errorf("file %q size changed from %d to %d", name, metadata.Size, size)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &File{
		downloadID: s.downloadID, storeURL: s.storeURL,
		globalOffset: metadata.GlobalOffset, size: metadata.Size, client: s.client,
		ctx: ctx, cancel: cancel,
	}, s.existing, nil
}

func (s *Storage) RootDir() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.downloadID == 0 {
		return "pos://pending/" + s.torrentID
	}
	return "pos://" + strconv.FormatInt(s.downloadID, 10)
}

func (s *Storage) PreserveOnRemove() bool { return true }

type File struct {
	downloadID   int64
	storeURL     string
	globalOffset int64
	size         int64
	client       *http.Client
	ctx          context.Context
	cancel       context.CancelFunc
}

func (f *File) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off > f.size-int64(len(p)) {
		return 0, fmt.Errorf("write range [%d,%d) exceeds file size %d", off, off+int64(len(p)), f.size)
	}
	if len(p) == 0 {
		return 0, nil
	}
	var written int
	for written < len(p) {
		length := min(len(p)-written, maxPOSRangeSize)
		n, err := f.writeRange(p[written:written+length], off+int64(written))
		written += n
		if err != nil {
			return written, err
		}
		if n != length {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// WriteVerifiedAt records bytes that Rain read from POS and successfully
// verified against the torrent piece hash. This fills any sparse zero ranges
// after Rain restarts without changing the normal download write path.
func (f *File) WriteVerifiedAt(p []byte, off int64) (int, error) {
	return f.WriteAt(p, off)
}

func (f *File) writeRange(p []byte, off int64) (int, error) {
	u := f.writeURL(off, int64(len(p)))
	resp, err := doRequest(f.ctx, f.client, http.MethodPatch, u, "application/octet-stream", p)
	if err != nil {
		return 0, fmt.Errorf("writing POS range: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return 0, fmt.Errorf("writing POS range: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(message)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return len(p), nil
}

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("negative read offset %d", off)
	}
	if off >= f.size {
		return 0, io.EOF
	}
	want := len(p)
	if int64(want) > f.size-off {
		want = int(f.size - off)
	}
	var read int
	for read < want {
		length := min(want-read, maxPOSRangeSize)
		n, err := f.readRange(p[read:read+length], off+int64(read))
		read += n
		if err != nil {
			return read, err
		}
		if n != length {
			return read, io.ErrUnexpectedEOF
		}
	}
	if len(p) > want {
		return read, io.EOF
	}
	return read, nil
}

func (f *File) readRange(p []byte, off int64) (int, error) {
	resp, err := doRequest(f.ctx, f.client, http.MethodGet, f.readURL(off, int64(len(p))), "", nil)
	if err != nil {
		return 0, fmt.Errorf("reading POS range: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return 0, fmt.Errorf("reading POS range: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(message)))
	}
	n, err := io.ReadFull(resp.Body, p)
	if err != nil {
		return n, err
	}
	return n, nil
}

func doRequest(ctx context.Context, client *http.Client, method, requestURL, contentType string, body []byte) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		delay := retryDelays[min(attempt, len(retryDelays)-1)]
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, requestURL, reader)
		if err != nil {
			return nil, err
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			continue
		}
		switch resp.StatusCode {
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			continue
		}
		return resp, nil
	}
}

func (f *File) writeURL(off, length int64) string {
	global := f.globalOffset + off
	return f.storeURL + "/v2/rain/downloads/" + strconv.FormatInt(f.downloadID, 10) +
		"/blob?offset=" + url.QueryEscape(strconv.FormatInt(global, 10)) +
		"&length=" + url.QueryEscape(strconv.FormatInt(length, 10))
}

func (f *File) readURL(off, length int64) string {
	global := f.globalOffset + off
	return f.storeURL + "/v2/rain/downloads/" + strconv.FormatInt(f.downloadID, 10) +
		"?offset=" + url.QueryEscape(strconv.FormatInt(global, 10)) +
		"&length=" + url.QueryEscape(strconv.FormatInt(length, 10))
}

func (f *File) Close() error {
	f.cancel()
	return nil
}

var _ storage.Provider = (*Provider)(nil)
var _ storage.Storage = (*Storage)(nil)
var _ storage.Preparer = (*Storage)(nil)
var _ storage.PreserveOnRemove = (*Storage)(nil)
var _ storage.File = (*File)(nil)
