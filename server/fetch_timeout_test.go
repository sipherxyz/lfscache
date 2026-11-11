package server

import (
	"crypto/sha256"
	"encoding/hex"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/saracen/lfscache/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFetchStalledUpstream checks that an upstream that stops sending data
// does not keep the object in flight forever: the fetch fails after the idle
// timeout and the next request for the object fetches it again.
func TestFetchStalledUpstream(t *testing.T) {
	data := []byte("stalled object content")
	sum := sha256.Sum256(data)
	oid := hex.EncodeToString(sum[:])

	release := make(chan struct{})
	stall := true
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stall {
			w.Write(data[:5])
			w.(http.Flusher).Flush()
			<-release
			return
		}
		w.Write(data)
	}))
	defer upstream.Close()
	defer close(release)

	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := New(log.NewNopLogger(), upstream.URL, dir)
	require.NoError(t, err)
	s.FetchIdleTimeout = 200 * time.Millisecond

	cr, cw, source, err := s.cache.Get(oid)
	require.NoError(t, err)
	require.Equal(t, cache.SourceFresh, source)
	cr.Close()

	done := make(chan error, 1)
	go func() { done <- s.fetch(cw, oid, upstream.URL, len(data), http.Header{}) }()

	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("fetch from a stalled upstream did not time out")
	}

	// the failed fetch must not be left in flight
	stall = false
	cr, cw, source, err = s.cache.Get(oid)
	require.NoError(t, err)
	assert.Equal(t, cache.SourceFresh, source)
	cr.Close()
	require.NoError(t, s.fetch(cw, oid, upstream.URL, len(data), http.Header{}))

	cr, _, source, err = s.cache.Get(oid)
	require.NoError(t, err)
	assert.Equal(t, cache.SourceDisk, source)
	cr.Close()
}
