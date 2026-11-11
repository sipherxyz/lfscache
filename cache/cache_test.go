package cache

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	c, err := NewFilesystemCache(dir)
	require.NoError(t, err)

	write := func(key string) error {
		cr, cw, source, err := c.Get(key)
		if err != nil {
			return err
		}

		if source != SourceFresh {
			return fmt.Errorf("expected source to be %v, got %v", SourceFresh, source)
		}

		if _, err := cw.Write([]byte("foobar")); err != nil {
			return err
		}

		return cr.Close()
	}

	read := func(key string, expectedSource Source) error {
		cr, cw, source, err := c.Get(key)
		if err != nil {
			return err
		}

		if cw != nil {
			return fmt.Errorf("expected writer to be nil")
		}

		if source != expectedSource {
			return fmt.Errorf("expected source to be %v, got %v", expectedSource, source)
		}

		p := make([]byte, 6)
		n, err := cr.Read(p)
		if err != nil {
			return err
		}

		if n != len(p) {
			return fmt.Errorf("expected length to be %d, got %d", len(p), n)
		}

		return cr.Close()
	}

	require.NoError(t, write("foobar"))
	require.NoError(t, read("foobar", SourceInflight))
	require.NoError(t, c.Done("foobar", nil))
	require.FileExists(t, filepath.Join(dir, DirObjects, DefaultFilenamer("foobar")))

	require.EqualError(t, c.Done("foobar", nil), ErrKeyNotFound.Error())

	require.NoError(t, read("foobar", SourceDisk))
	require.NoError(t, write("hello"))
	require.NoError(t, c.Done("hello", fmt.Errorf("fake error")))

	_, err = os.Stat(filepath.Join(dir, DirObjects, DefaultFilenamer("hello")))
	require.Error(t, err)
}

// TestDoneDoesNotBlockOtherKeys checks that Done, which waits for the readers
// of a key to close, does not block Get for other keys meanwhile.
func TestDoneDoesNotBlockOtherKeys(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	fc, err := NewFilesystemCache(dir)
	require.NoError(t, err)

	slowReader, w, _, err := fc.Get("aaaa1111")
	require.NoError(t, err)
	_, err = w.Write([]byte("data"))
	require.NoError(t, err)

	doneErr := make(chan error, 1)
	go func() { doneErr <- fc.Done("aaaa1111", nil) }()

	got := make(chan error, 1)
	go func() {
		r, w, source, err := fc.Get("bbbb2222")
		if err == nil {
			assert.Equal(t, SourceFresh, source)
			r.Close()
			err = fc.Done("bbbb2222", nil)
			_ = w
		}
		got <- err
	}()

	select {
	case err := <-got:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Get for another key blocked while Done waited for a reader")
	}

	slowReader.Close()
	require.NoError(t, <-doneErr)
}

// TestRefetchWhileClosing checks that a new fetch of a key whose previous
// fetch is still closing does not truncate the file the old readers read.
func TestRefetchWhileClosing(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	fc, err := NewFilesystemCache(dir)
	require.NoError(t, err)

	oldReader, w, _, err := fc.Get("cccc3333")
	require.NoError(t, err)
	_, err = w.Write([]byte("old-data"))
	require.NoError(t, err)

	doneErr := make(chan error, 1)
	go func() { doneErr <- fc.Done("cccc3333", errors.New("upstream failed")) }()

	// wait until Done has removed the key, then start a new fetch
	var newReader ReadAtReadCloser
	require.Eventually(t, func() bool {
		r, _, source, err := fc.Get("cccc3333")
		if err != nil || source != SourceFresh {
			if r != nil {
				r.Close()
			}
			return false
		}
		newReader = r
		return true
	}, 2*time.Second, 5*time.Millisecond)
	defer newReader.Close()

	p := make([]byte, 8)
	n, err := oldReader.ReadAt(p, 0)
	assert.Equal(t, "old-data", string(p[:n]))
	assert.True(t, err == nil || err == io.EOF)

	oldReader.Close()
	require.NoError(t, <-doneErr)
}
