package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func server() (*httptest.Server, *Server, string, error) {
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/objects/batch":
			response := BatchResponse{
				Transfer: "basic",
				Objects: []*BatchObjectResponse{
					{
						OID:           "1111111",
						Size:          123,
						Authenticated: true,
						Actions: map[string]*BatchObjectActionResponse{
							"download": {
								Href: ts.URL + "/download",
							},
						},
					},
				},
			}

			json.NewEncoder(w).Encode(response)

		default:
			fmt.Fprintf(w, "upstream")
		}
	}))

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return ts, nil, dir, err
	}

	s, err := New(log.NewNopLogger(), ts.URL, dir)

	return ts, s, dir, err
}

func TestProxy(t *testing.T) {
	ts, s, dir, err := server()
	defer os.RemoveAll(dir)
	defer ts.Close()

	require.NoError(t, err)
	w := httptest.NewRecorder()

	req := httptest.NewRequest("POST", ts.URL+"/anything", nil)
	s.Handle().ServeHTTP(w, req)

	body, _ := ioutil.ReadAll(w.Body)
	assert.Equal(t, body, []byte("upstream"))
}

func TestBatch(t *testing.T) {
	ts, s, dir, err := server()
	defer os.RemoveAll(dir)
	defer ts.Close()

	require.NoError(t, err)
	w := httptest.NewRecorder()

	var br BatchResponse
	{
		req := httptest.NewRequest("POST", ts.URL+"/objects/batch", nil)
		s.Handle().ServeHTTP(w, req)

		require.NoError(t, json.NewDecoder(w.Body).Decode(&br))
	}

	require.Len(t, br.Objects, 1)
	require.Contains(t, br.Objects[0].Actions, "download")
	action := br.Objects[0].Actions["download"]

	req := httptest.NewRequest("POST", action.Href, nil)
	for key, val := range action.Header {
		req.Header.Add(key, val)
	}
	s.Handle().ServeHTTP(w, req)

	body, _ := ioutil.ReadAll(w.Body)
	assert.Equal(t, body, []byte("upstream"))
}

// upstreamWithMissing answers a batch with one downloadable object and one
// object that does not exist, the way GitHub LFS does.
func upstreamWithMissing(t *testing.T) *httptest.Server {
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/objects/batch":
			user, pass, _ := r.BasicAuth()
			w.Header().Set("X-Upstream-Auth", user+":"+pass)
			body, _ := ioutil.ReadAll(r.Body)
			w.Header().Set("X-Upstream-Body", string(body))
			json.NewEncoder(w).Encode(BatchResponse{
				Transfer: "basic",
				Objects: []*BatchObjectResponse{
					{
						OID:  "1111111",
						Size: 8,
						Actions: map[string]*BatchObjectActionResponse{
							"download": {
								Href:   ts.URL + "/download",
								Header: map[string]string{"Authorization": "RemoteAuth secret"},
							},
						},
					},
					{
						OID:   "2222222",
						Size:  100,
						Error: &BatchObjectError{Code: 404, Message: "Object does not exist on the server"},
					},
				},
			})
		case "/download":
			w.Header().Set("X-Upstream-Download-Auth", r.Header.Get("Authorization"))
			fmt.Fprintf(w, "upstream")
		default:
			http.NotFound(w, r)
		}
	}))
	return ts
}

func batchJSON(operation string) *bytes.Reader {
	return bytes.NewReader([]byte(`{"operation":"` + operation + `","transfers":["basic"],"objects":[{"oid":"1111111","size":8},{"oid":"2222222","size":100}]}`))
}

func TestBatchKeepsObjectError(t *testing.T) {
	ts := upstreamWithMissing(t)
	defer ts.Close()

	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := New(log.NewNopLogger(), ts.URL, dir)
	require.NoError(t, err)

	w := httptest.NewRecorder()
	s.Handle().ServeHTTP(w, httptest.NewRequest("POST", ts.URL+"/objects/batch", batchJSON("download")))

	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	objects := raw["objects"].([]interface{})
	require.Len(t, objects, 2)

	found := objects[0].(map[string]interface{})
	assert.Contains(t, found["actions"], "download")
	assert.Nil(t, found["error"])

	missing := objects[1].(map[string]interface{})
	_, hasActions := missing["actions"]
	assert.False(t, hasActions, "missing object must not carry actions")
	assert.Equal(t, map[string]interface{}{"code": float64(404), "message": "Object does not exist on the server"}, missing["error"])
}

func TestReadOnly(t *testing.T) {
	ts := upstreamWithMissing(t)
	defer ts.Close()

	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := NewReadOnly(log.NewNopLogger(), ts.URL, dir, "x-access-token", func() (string, error) { return "pat", nil })
	require.NoError(t, err)

	// uploads and anything but the batch endpoint are refused, including a
	// body whose duplicate "operation" key the upstream could read as upload
	for _, req := range []*http.Request{
		httptest.NewRequest("POST", ts.URL+"/objects/batch", batchJSON("upload")),
		httptest.NewRequest("POST", ts.URL+"/objects/batch", bytes.NewReader([]byte(`{"operation":"download","operation":"upload","objects":[]}`))),
		httptest.NewRequest("GET", ts.URL+"/", nil),
		httptest.NewRequest("GET", ts.URL+"/objects/batch", nil),
	} {
		w := httptest.NewRecorder()
		s.Handle().ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)
		assert.Contains(t, w.Body.String(), "downloads only")
	}

	// download batch: client credentials are replaced by the server token
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", ts.URL+"/objects/batch", batchJSON("download"))
	req.Header.Set("Authorization", "Basic Og==")
	s.Handle().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "x-access-token:pat", w.Header().Get("X-Upstream-Auth"))
	assert.JSONEq(t, `{"operation":"download","transfers":["basic"],"objects":[{"oid":"1111111","size":8},{"oid":"2222222","size":100}]}`, w.Header().Get("X-Upstream-Body"))

	var br BatchResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &br))
	require.Len(t, br.Objects, 2)

	// the missing object keeps its upstream error
	require.NotNil(t, br.Objects[1].Error)
	assert.Equal(t, 404, br.Objects[1].Error.Code)
	assert.Nil(t, br.Objects[1].Actions)

	// the found object gets a sealed href with no headers to echo
	action := br.Objects[0].Actions["download"]
	require.NotNil(t, action)
	assert.Empty(t, action.Header)
	assert.Equal(t, 3600, action.ExpiresIn)
	assert.Contains(t, action.Href, ContentCachePathPrefix+"1111111?token=")

	w = httptest.NewRecorder()
	s.Handle().ServeHTTP(w, httptest.NewRequest("GET", action.Href, nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "upstream", w.Body.String())

	// a tampered or foreign token is refused
	w = httptest.NewRecorder()
	s.Handle().ServeHTTP(w, httptest.NewRequest("GET", action.Href+"x", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)

	w = httptest.NewRecorder()
	s.Handle().ServeHTTP(w, httptest.NewRequest("GET", ts.URL+ContentCachePathPrefix+"1111111", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCIDRAllowlist(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h, err := NewCIDRAllowlist(ok, []string{"172.30.22.0/23"}, []string{"172.19.0.1/32"})
	require.NoError(t, err)

	cases := []struct {
		remote, forwarded string
		code              int
	}{
		{"172.30.23.5:1234", "", http.StatusNoContent},
		{"10.0.0.1:1234", "", http.StatusForbidden},
		{"172.19.0.1:1234", "172.30.22.9", http.StatusNoContent},
		{"172.19.0.1:1234", "172.30.22.9, 8.8.8.8", http.StatusForbidden},
		{"172.19.0.1:1234", "8.8.8.8, 172.30.22.9, 172.19.0.1", http.StatusNoContent},
		{"172.19.0.1:1234", "172.30.22.9|8.8.8.8", http.StatusForbidden},
		{"172.19.0.1:1234", "", http.StatusForbidden},
		{"10.0.0.1:1234", "172.30.22.9", http.StatusForbidden},
	}
	for _, c := range cases {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = c.remote
		for _, line := range strings.Split(c.forwarded, "|") {
			if line != "" {
				req.Header.Add("X-Forwarded-For", line)
			}
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		assert.Equal(t, c.code, w.Code, "%s / %q", c.remote, c.forwarded)
	}
}
