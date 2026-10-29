package server

import (
	"errors"
	"net"
	"net/http"
	"strings"
)

func parseCIDRs(cidrs []string) ([]*net.IPNet, error) {
	var nets []*net.IPNet
	for _, cidr := range cidrs {
		cidr = strings.TrimSpace(cidr)
		if cidr == "" {
			continue
		}
		_, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, err
		}
		nets = append(nets, ipnet)
	}
	return nets, nil
}

func contains(nets []*net.IPNet, ip net.IP) bool {
	for _, ipnet := range nets {
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

func remoteIP(remoteAddr string) net.IP {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	return net.ParseIP(host)
}

// rightmostForwardedIP returns the last address of an X-Forwarded-For header,
// which is the one appended by the proxy that sent us the request.
func rightmostForwardedIP(forwardedFor string) net.IP {
	parts := strings.Split(forwardedFor, ",")
	return net.ParseIP(strings.TrimSpace(parts[len(parts)-1]))
}

// NewCIDRAllowlist wraps next so that only clients inside allowed may use it.
// When the direct peer is inside trustedProxies, the client address is taken
// from the rightmost X-Forwarded-For entry instead.
func NewCIDRAllowlist(next http.Handler, allowed, trustedProxies []string) (http.Handler, error) {
	allowedNets, err := parseCIDRs(allowed)
	if err != nil {
		return nil, err
	}
	if len(allowedNets) == 0 {
		return nil, errors.New("no allowed client CIDRs")
	}
	proxyNets, err := parseCIDRs(trustedProxies)
	if err != nil {
		return nil, err
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := remoteIP(r.RemoteAddr)
		if ip != nil && contains(proxyNets, ip) {
			if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
				ip = rightmostForwardedIP(forwarded)
			}
		}
		if ip == nil || !contains(allowedNets, ip) {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	}), nil
}
