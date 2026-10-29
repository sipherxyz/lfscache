package server

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/textproto"
	"strings"
	"time"
)

// downloadTokenTTL is how long a sealed download href stays valid.
const downloadTokenTTL = time.Hour

// downloadToken is the sealed payload carried in the "token" query parameter
// of a cached download href. It holds everything the cache needs to fetch the
// object from the upstream, including the upstream authorization header, so
// it is encrypted (AES-GCM) with a per-process key rather than only signed.
type downloadToken struct {
	OID       string            `json:"oid"`
	Size      int64             `json:"size"`
	URL       string            `json:"url"`
	Header    map[string]string `json:"header,omitempty"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// TokenFromFile returns a token provider which reads and trims the file on
// every call, so the upstream credential can be rotated without a restart.
func TokenFromFile(path string) func() (string, error) {
	return func() (string, error) {
		b, err := ioutil.ReadFile(path)
		if err != nil {
			return "", err
		}
		token := strings.TrimSpace(string(b))
		if token == "" {
			return "", fmt.Errorf("upstream token file %s is empty", path)
		}
		return token, nil
	}
}

func (s *Server) aead() (cipher.AEAD, error) {
	block, err := aes.NewCipher(s.hmacKey[:32])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// sealAction encrypts the upstream download details into an opaque token.
func (s *Server) sealAction(oid string, size int64, href string, header map[string]string, expiresAt time.Time) (string, error) {
	payload, err := json.Marshal(downloadToken{
		OID:       oid,
		Size:      size,
		URL:       href,
		Header:    header,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return "", err
	}

	gcm, err := s.aead()
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}

	sealed := gcm.Seal(nonce, nonce, payload, []byte(oid))
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

// openAction decrypts a token produced by sealAction and validates it against
// the requested oid and the current time.
func (s *Server) openAction(oid, token string) (url string, size int, header http.Header, err error) {
	sealed, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", 0, nil, err
	}

	gcm, err := s.aead()
	if err != nil {
		return "", 0, nil, err
	}
	if len(sealed) < gcm.NonceSize() {
		return "", 0, nil, errors.New("invalid token")
	}

	payload, err := gcm.Open(nil, sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():], []byte(oid))
	if err != nil {
		return "", 0, nil, errors.New("invalid token")
	}

	var dt downloadToken
	if err = json.Unmarshal(payload, &dt); err != nil {
		return "", 0, nil, err
	}
	if dt.OID != oid {
		return "", 0, nil, errors.New("token oid mismatch")
	}
	if time.Now().After(dt.ExpiresAt) {
		return "", 0, nil, errors.New("token expired")
	}

	header = make(http.Header, len(dt.Header))
	for key, value := range dt.Header {
		header.Set(textproto.CanonicalMIMEHeaderKey(key), value)
	}

	return dt.URL, int(dt.Size), header, nil
}
