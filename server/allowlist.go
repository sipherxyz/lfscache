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

// rightmostForwardedIP walks every X-Forwarded-For entry from the right,
// skipping trusted proxies, and returns the first address that was not
// appended by a proxy we trust. Entries further left are client-supplied and
// cannot be trusted.
func rightmostForwardedIP(forwardedFor []string, trustedProxies []*net.IPNet) net.IP {
	entries := strings.Split(strings.Join(forwardedFor, ","), ",")
	for i := len(entries) - 1; i >= 0; i-- {
		ip := net.ParseIP(strings.TrimSpace(entries[i]))
		if ip == nil {
			return nil
		}
		if !contains(trustedProxies, ip) {
			return ip
		}
	}
	return nil
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
			ip = rightmostForwardedIP(r.Header.Values("X-Forwarded-For"), proxyNets)
		}
		if ip == nil || !contains(allowedNets, ip) {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	}), nil
}
