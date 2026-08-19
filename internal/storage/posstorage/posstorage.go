package posstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/rain/v2/internal/logger"
	"github.com/cenkalti/rain/v2/internal/storage"
)

type Provider struct {
	controllerURL string
	client        *http.Client
	stateStore    StateStore
}

const maxPOSRangeSize = 128 << 20

// StateStore persists opaque POS registration state. POS storage owns the
// bytes and their format.
type StateStore interface {
	ReadStorageState(torrentID string) ([]byte, error)
	WriteStorageState(torrentID string, state []byte) error
}

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

func NewProvider(controllerURL string, timeout time.Duration) (*Provider, error) {
	controllerURL = strings.TrimRight(controllerURL, "/")
	parsed, err := url.Parse(controllerURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("invalid POS controller URL")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 32
	return &Provider{
		controllerURL: controllerURL,
		client:        &http.Client{Transport: transport, Timeout: timeout},
	}, nil
}

func (p *Provider) GetStorage(torrentID string) (storage.Storage, error) {
	if torrentID == "" {
		return nil, fmt.Errorf("empty torrent id")
	}
	var state []byte
	if p.stateStore != nil {
		var err error
		state, err = p.stateStore.ReadStorageState(torrentID)
		if err != nil {
			return nil, err
		}
	}
	registrationCtx, cancelRegistration := context.WithCancel(context.Background())
	s := &Storage{
		controllerURL: p.controllerURL,
		torrentID:     torrentID,
		client:        p.client,
		log:           logger.New("posstorage-" + torrentID),
		files:         make(map[string]fileMetadata),
		persist: func(value []byte) error {
			if p.stateStore == nil {
				return nil
			}
			return p.stateStore.WriteStorageState(torrentID, value)
		},
		registrationCtx:    registrationCtx,
		cancelRegistration: cancelRegistration,
	}
	if len(state) == 0 {
		return s, nil
	}
	var restored persistedRegistration
	if err := json.Unmarshal(state, &restored); err != nil {
		return nil, fmt.Errorf("decoding POS registration state: %w", err)
	}
	if restored.DownloadID < 0 {
		return nil, fmt.Errorf("invalid persisted POS download id %d", restored.DownloadID)
	}
	if restored.Rejected && restored.DownloadID != 0 {
		return nil, fmt.Errorf("persisted POS registration is rejected but has download id %d", restored.DownloadID)
	}
	if len(restored.Body) > 0 {
		if err := json.Unmarshal(restored.Body, &s.registrationRequest); err != nil {
			return nil, fmt.Errorf("decoding persisted POS registration body: %w", err)
		}
		if s.registrationRequest.RainTorrentID != torrentID {
			return nil, fmt.Errorf("persisted POS registration has torrent id %q, want %q", s.registrationRequest.RainTorrentID, torrentID)
		}
		s.registrationBody = bytes.Clone(restored.Body)
	}
	if len(s.registrationBody) > 0 || restored.DownloadID > 0 || restored.Rejected {
		s.registrationState = registrationCompleted
	}
	s.downloadID = restored.DownloadID
	s.registrationRejected = restored.Rejected
	return s, nil
}

// SetStateStore configures durable registration state before any torrent is
// restored or started.
func (p *Provider) SetStateStore(store StateStore) {
	p.stateStore = store
}

type Storage struct {
	// registrationMu orders Prepare and Cancel. If registration is running,
	// cancellation waits for its outcome before acting on the returned ID.
	registrationMu        sync.Mutex
	canceled              bool
	registrationState     registrationState
	registrationRequest   createRequest
	registrationBody      []byte
	registrationRejected  bool
	cancellationConfirmed bool
	registrationCtx       context.Context
	cancelRegistration    context.CancelFunc
	persist               func([]byte) error

	mu            sync.RWMutex
	controllerURL string
	torrentID     string
	downloadID    int64
	storeURL      string
	existing      bool
	prepared      bool
	files         map[string]fileMetadata
	client        *http.Client
	log           logger.Logger
}

type registrationState uint8

const (
	registrationNotStarted registrationState = iota
	registrationRunning
	registrationCompleted
)

type persistedRegistration struct {
	Body       []byte `json:"body,omitempty"`
	DownloadID int64  `json:"download_id,omitempty"`
	Rejected   bool   `json:"rejected,omitempty"`
}

var errRegistrationRejected = errors.New("POS registration definitively rejected")

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
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()
	if s.canceled {
		return fmt.Errorf("POS storage registration canceled")
	}
	if s.registrationRejected {
		return errRegistrationRejected
	}
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
	if s.registrationBody == nil {
		if manifest.TorrentID != s.torrentID {
			return fmt.Errorf("POS manifest has torrent id %q, want %q", manifest.TorrentID, s.torrentID)
		}
		request := createRequest{
			RainTorrentID: s.torrentID,
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
		s.registrationRequest = request
		s.registrationBody = body
		if err := s.persistRegistration(s.POSDownloadID()); err != nil {
			// No POST has started. Forget the in-memory body so a later Prepare
			// must persist it successfully before trying registration.
			s.registrationRequest = createRequest{}
			s.registrationBody = nil
			return fmt.Errorf("persisting POS registration request: %w", err)
		}
	}

	s.registrationState = registrationRunning
	requestCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.registrationCtx, cancel)
	result, existing, err := s.register(requestCtx, true)
	stop()
	cancel()
	s.registrationState = registrationCompleted
	if err != nil {
		if errors.Is(err, errRegistrationRejected) && s.POSDownloadID() == 0 {
			s.registrationRejected = true
			if persistErr := s.persistRegistration(0); persistErr != nil {
				return errors.Join(err, fmt.Errorf("persisting rejected POS registration: %w", persistErr))
			}
		}
		return err
	}
	if downloadID := s.POSDownloadID(); downloadID != 0 && downloadID != result.ID {
		return fmt.Errorf("POS download id changed from %d to %d", downloadID, result.ID)
	}
	if err := s.persistRegistration(result.ID); err != nil {
		return fmt.Errorf("persisting POS download id %d: %w", result.ID, err)
	}
	s.saveRegistration(result, existing)
	return nil
}

func (s *Storage) persistRegistration(downloadID int64) error {
	state, err := json.Marshal(persistedRegistration{
		Body:       s.registrationBody,
		DownloadID: downloadID,
		Rejected:   s.registrationRejected,
	})
	if err != nil {
		return err
	}
	return s.persist(state)
}

func (s *Storage) register(ctx context.Context, retry bool) (createResponse, bool, error) {
	var resp *http.Response
	var err error
	var retryStats registrationRetryStats
	if retry {
		resp, retryStats, err = s.doRequest(ctx, http.MethodPost, s.controllerURL+"/v2/rain/downloads", "application/json", s.registrationBody)
	} else {
		resp, err = doRequestOnce(ctx, s.client, http.MethodPost, s.controllerURL+"/v2/rain/downloads", "application/json", s.registrationBody)
	}
	if err != nil {
		return createResponse{}, false, fmt.Errorf("registering POS rain download: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if resp.StatusCode == http.StatusBadRequest {
			// The POS controller contract guarantees that HTTP 400 rejects the
			// request before creating a download.
			return createResponse{}, false, fmt.Errorf("%w: HTTP %d: %s", errRegistrationRejected, resp.StatusCode, strings.TrimSpace(string(message)))
		}
		return createResponse{}, false, fmt.Errorf("registering POS rain download: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(message)))
	}
	var result createResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return createResponse{}, false, fmt.Errorf("decoding POS rain download: %w", err)
	}
	if result.ID <= 0 || result.StoreURL == "" {
		return createResponse{}, false, fmt.Errorf("POS returned incomplete rain download identity")
	}
	if err := validateCreateResponse(s.registrationRequest, result); err != nil {
		return createResponse{}, false, fmt.Errorf("validating POS rain download: %w", err)
	}
	if retryStats.failures > 0 {
		s.log.Infof("POS registration recovered: torrent_id=%s attempts=%d elapsed=%s",
			s.torrentID, retryStats.attempts, time.Since(retryStats.started).Truncate(time.Second))
	}
	return result, resp.StatusCode == http.StatusOK, nil
}

type registrationRetryStats struct {
	started  time.Time
	attempts int
	failures int
}

func (s *Storage) doRequest(ctx context.Context, method, requestURL, contentType string, body []byte) (*http.Response, registrationRetryStats, error) {
	stats := registrationRetryStats{started: time.Now()}
	var failures int
	var lastFailureLog time.Time
	for attempt := 0; ; attempt++ {
		stats.attempts = attempt + 1
		delay := retryDelays[min(attempt, len(retryDelays)-1)]
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, stats, ctx.Err()
			case <-time.After(delay):
			}
		}
		resp, err := doRequestOnce(ctx, s.client, method, requestURL, contentType, body)
		if err != nil {
			if ctx.Err() != nil {
				return nil, stats, err
			}
			failures++
			stats.failures = failures
			now := time.Now()
			if lastFailureLog.IsZero() || now.Sub(lastFailureLog) >= time.Minute {
				retryIn := retryDelays[min(attempt+1, len(retryDelays)-1)]
				s.log.Warningf("POS registration unavailable: torrent_id=%s status=transport_error error=%q retry_in=%s attempt=%d",
					s.torrentID, err, retryIn, attempt+1)
				lastFailureLog = now
			}
			continue
		}
		switch resp.StatusCode {
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			message, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			failures++
			stats.failures = failures
			now := time.Now()
			if lastFailureLog.IsZero() || now.Sub(lastFailureLog) >= time.Minute {
				retryIn := retryDelays[min(attempt+1, len(retryDelays)-1)]
				s.log.Warningf("POS registration unavailable: torrent_id=%s status=%d error=%q retry_in=%s attempt=%d",
					s.torrentID, resp.StatusCode, strings.TrimSpace(string(message)), retryIn, attempt+1)
				lastFailureLog = now
			}
			continue
		}
		return resp, stats, nil
	}
}

func (s *Storage) saveRegistration(result createResponse, existing bool) {
	files := make(map[string]fileMetadata)
	for _, file := range result.Metadata.Files {
		if !file.Padding {
			files[file.Path] = file
		}
	}
	s.mu.Lock()
	s.downloadID = result.ID
	s.storeURL = strings.TrimRight(result.StoreURL, "/")
	s.existing = existing
	s.files = files
	s.prepared = true
	s.mu.Unlock()
}

// Cancel prevents future registration and confirms that any registration
// which may already have reached POS is canceled. registrationMu makes this
// operation idempotent and makes concurrent callers share one outcome.
func (s *Storage) Cancel(ctx context.Context) error {
	// This signal does not require registrationMu, so it interrupts Prepare's
	// active HTTP request before Cancel waits for Prepare to release the mutex.
	s.cancelRegistration()
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()
	s.canceled = true
	if s.cancellationConfirmed {
		return nil
	}
	if s.registrationRejected {
		s.cancellationConfirmed = true
		return nil
	}
	s.mu.RLock()
	downloadID := s.downloadID
	s.mu.RUnlock()
	if downloadID == 0 && len(s.registrationBody) == 0 {
		// The registration body is persisted before any POST. Without a body
		// or an ID, no POS download can have been created.
		s.cancellationConfirmed = true
		return nil
	}
	if downloadID == 0 {
		var err error
		downloadID, err = s.recoverRegistration(ctx)
		if err != nil {
			return err
		}
		if s.registrationRejected {
			s.cancellationConfirmed = true
			return nil
		}
	}
	if err := s.cancelDownload(ctx, downloadID); err != nil {
		return err
	}
	s.cancellationConfirmed = true
	return nil
}

func (s *Storage) recoverRegistration(ctx context.Context) (int64, error) {
	// A failed registration is ambiguous: POS may have committed it and lost
	// the response. Repeat the same idempotent request to recover its ID.
	s.registrationState = registrationRunning
	result, existing, err := s.register(ctx, false)
	s.registrationState = registrationCompleted
	if err != nil {
		if errors.Is(err, errRegistrationRejected) {
			s.registrationRejected = true
			if persistErr := s.persistRegistration(0); persistErr != nil {
				return 0, errors.Join(err, fmt.Errorf("persisting rejected POS registration: %w", persistErr))
			}
			return 0, nil
		}
		return 0, fmt.Errorf("recovering ambiguous POS registration: %w", err)
	}
	if err := s.persistRegistration(result.ID); err != nil {
		return 0, fmt.Errorf("persisting recovered POS download id %d: %w", result.ID, err)
	}
	s.saveRegistration(result, existing)
	return result.ID, nil
}

func (s *Storage) cancelDownload(ctx context.Context, downloadID int64) error {
	requestURL := s.controllerURL + "/v2/rain/downloads/" + strconv.FormatInt(downloadID, 10) + "/cancel"
	resp, err := doRequestOnce(ctx, s.client, http.MethodPost, requestURL, "", nil)
	if err != nil {
		return fmt.Errorf("canceling POS rain download %d: %w", downloadID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return fmt.Errorf("canceling POS rain download %d: HTTP %d: %s", downloadID, resp.StatusCode, strings.TrimSpace(string(message)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
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

// POSDownloadID returns the numeric POS download ID, or zero until registration completes.
func (s *Storage) POSDownloadID() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.downloadID
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
	resp, err := doRetriedRequest(f.ctx, f.client, http.MethodPatch, u, "application/octet-stream", p)
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
	requestURL := f.readURL(off, int64(len(p)))
	for attempt := 0; ; attempt++ {
		delay := retryDelays[min(attempt, len(retryDelays)-1)]
		if delay > 0 {
			select {
			case <-f.ctx.Done():
				return 0, f.ctx.Err()
			case <-time.After(delay):
			}
		}
		resp, err := doRetriedRequest(f.ctx, f.client, http.MethodGet, requestURL, "", nil)
		if err != nil {
			return 0, fmt.Errorf("reading POS range: %w", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			message, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			return 0, fmt.Errorf("reading POS range: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(message)))
		}
		n, err := io.ReadFull(resp.Body, p)
		resp.Body.Close()
		if err == nil {
			return n, nil
		}
		// A body that ends early or breaks mid-read is a connection failure
		// after a good status; retry it like any transport error.
	}
}

func doRetriedRequest(ctx context.Context, client *http.Client, method, requestURL, contentType string, body []byte) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		delay := retryDelays[min(attempt, len(retryDelays)-1)]
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		resp, err := doRequestOnce(ctx, client, method, requestURL, contentType, body)
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

func doRequestOnce(ctx context.Context, client *http.Client, method, requestURL, contentType string, body []byte) (*http.Response, error) {
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
	return client.Do(req)
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
var _ storage.Canceler = (*Storage)(nil)
var _ storage.POSDownloadIDProvider = (*Storage)(nil)
var _ storage.PreserveOnRemove = (*Storage)(nil)
var _ storage.File = (*File)(nil)
