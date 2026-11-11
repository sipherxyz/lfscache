package cache

import (
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentReadWriter(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	defer f.Close()

	crw := NewConcurrentReadWriter(f)

	read := func(size int64, expected []byte, offset int64, closed bool) {
		r := crw.Reader()
		defer r.Close()

		if closed {
			r.Close()
		}

		p := make([]byte, size)

		var n int
		var err error
		if offset == 0 {
			n, err = r.Read(p)
		} else {
			n, err = r.ReadAt(p, offset)
		}

		if closed && err == io.EOF {
			return
		}
		if err != io.EOF && assert.NoError(t, err) {
			return
		}

		assert.Equal(t, p[:n], expected)
	}

	go read(10, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0, false) // read from start
	go read(5, []byte{100, 101, 102, 103, 104}, 100, false)     // read from middle
	go read(5, []byte{100, 101, 102, 103, 104}, 0, true)        // read from middle with reader closed
	go read(5, []byte{254, 255}, 254, false)                    // read at end and request more data than available
	go read(10, []byte{}, 300, false)                           // read past end

	for i := 0; i < 256; i++ {
		time.Sleep(time.Millisecond)
		crw.Write([]byte{byte(i)})
	}
	crw.Close()
}

// TestConcurrentReadWriterCloseWakesReader races a reader that reaches the
// end of the data against the writer's Close. A Close between the reader's EOF
// and its wait must still wake the reader; otherwise the reader never closes
// and Close (and so FilesystemCache.Done) blocks forever.
func TestConcurrentReadWriterCloseWakesReader(t *testing.T) {
	for i := 0; i < 5000; i++ {
		f, err := ioutil.TempFile("", "")
		require.NoError(t, err)

		crw := NewConcurrentReadWriter(f)
		r := crw.Reader()

		go func() {
			ioutil.ReadAll(r)
			r.Close()
		}()

		_, err = crw.Write([]byte("x"))
		require.NoError(t, err)
		if i%2 == 0 {
			runtime.Gosched()
		}

		closed := make(chan error, 1)
		go func() { closed <- crw.Close() }()

		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Close blocked, reader was not woken", i)
		}
		os.Remove(f.Name())
	}
}
