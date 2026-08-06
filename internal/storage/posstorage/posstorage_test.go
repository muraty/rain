package posstorage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFileWritesMapFileOffsetToTorrentOffset(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(*File, []byte, int64) (int, error)
	}{
		{name: "download", write: (*File).WriteAt},
		{name: "restart verification", write: (*File).WriteVerifiedAt},
	} {
		t.Run(test.name, func(t *testing.T) {
			var gotPath, gotOffset, gotLength, gotBody string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotOffset = r.URL.Query().Get("offset")
				gotLength = r.URL.Query().Get("length")
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("reading request body: %s", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				gotBody = string(body)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			file := &File{
				downloadID: 7, storeURL: server.URL, globalOffset: 100, size: 200,
				client: server.Client(), ctx: ctx, cancel: cancel,
			}
			n, err := test.write(file, []byte("rain"), 23)
			if err != nil {
				t.Fatal(err)
			}
			if n != 4 {
				t.Fatalf("wrote %d bytes, want 4", n)
			}
			if gotPath != "/v2/rain/downloads/7/blob" || gotOffset != "123" || gotLength != "4" || gotBody != "rain" {
				t.Fatalf("request = path %q offset %q length %q body %q", gotPath, gotOffset, gotLength, gotBody)
			}
		})
	}
}

func TestFileWriteRejectsRangeOutsideFile(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	file := &File{
		downloadID: 7, storeURL: server.URL, globalOffset: 100, size: 4,
		client: server.Client(), ctx: ctx, cancel: cancel,
	}
	if _, err := file.WriteAt([]byte("too long"), 0); err == nil {
		t.Fatal("expected an out-of-range write to fail")
	}
	if requests != 0 {
		t.Fatalf("out-of-range write sent %d requests, want 0", requests)
	}
}
