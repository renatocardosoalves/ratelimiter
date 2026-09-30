package middleware

import (
	"errors"
	"net"
	"net/http"
	"strings"
)

// ErrEmptyKey is returned by a KeyFunc when the client key cannot be
// determined from the request.
var ErrEmptyKey = errors.New("middleware: empty client key")

// KeyFunc extracts the key that identifies the client of a request.
type KeyFunc func(r *http.Request) (string, error)

// KeyByIP identifies the client by the IP address of the TCP connection
// (http.Request.RemoteAddr). This is the default KeyFunc.
func KeyByIP(r *http.Request) (string, error) {
	return ipFromAddr(r.RemoteAddr)
}

// KeyByForwardedIP identifies the client by the first IP found in the
// X-Forwarded-For header, falling back to X-Real-IP and then to RemoteAddr.
//
// Only use it when the application runs behind a trusted reverse proxy that
// sets these headers, otherwise clients can spoof their identity.
func KeyByForwardedIP(r *http.Request) (string, error) {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		if ip := strings.TrimSpace(first); ip != "" {
			return ip, nil
		}
	}
	if ip := strings.TrimSpace(r.Header.Get("X-Real-IP")); ip != "" {
		return ip, nil
	}
	return KeyByIP(r)
}

// KeyByHeader identifies the client by the value of the given header, e.g.
// an API token ("Authorization", "X-API-Key") or "User-Agent". Requests
// without the header produce ErrEmptyKey.
func KeyByHeader(name string) KeyFunc {
	return func(r *http.Request) (string, error) {
		if v := r.Header.Get(name); v != "" {
			return v, nil
		}
		return "", ErrEmptyKey
	}
}

func ipFromAddr(addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = strings.Trim(addr, "[]")
	}
	if host == "" {
		return "", ErrEmptyKey
	}
	return host, nil
}
