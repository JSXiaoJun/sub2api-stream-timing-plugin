package timing

import (
	"crypto/tls"
	"errors"
	"fmt"
	"golang.org/x/net/http2"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type poolEntry struct {
	client  *http.Client
	tr      *http.Transport
	used    uint64
	active  int
	retired bool
}
type transportPool struct {
	mu      sync.Mutex
	entries map[string]*poolEntry
	tick    uint64
	cfg     Config
	closed  bool
}

func newPool(c Config) *transportPool {
	return &transportPool{cfg: c, entries: make(map[string]*poolEntry)}
}

func (p *transportPool) acquire(proxy string, account int64) (*http.Client, func(), error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, nil, errors.New("transport closed")
	}
	key := fmt.Sprintf("%d|%s", account, proxy)
	p.tick++
	e := p.entries[key]
	if e == nil {
		var u *url.URL
		if proxy != "" {
			var err error
			u, err = url.Parse(proxy)
			if err != nil || u.Host == "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" && u.Scheme != "socks5h") {
				return nil, nil, errors.New("invalid proxy")
			}
		}
		c := p.cfg
		tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Duration(c.DialTimeoutSeconds) * time.Second, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: c.EnableHTTP2, DisableCompression: true, MaxIdleConns: c.IdleConnectionsPerPool, MaxIdleConnsPerHost: c.IdleConnectionsPerPool, IdleConnTimeout: time.Duration(c.IdleTimeoutSeconds) * time.Second, TLSHandshakeTimeout: time.Duration(c.TLSHandshakeTimeoutSeconds) * time.Second, ResponseHeaderTimeout: time.Duration(c.ResponseHeaderTimeoutSeconds) * time.Second}
		if u != nil {
			tr.Proxy = http.ProxyURL(u)
		}
		if c.EnableHTTP2 {
			h, err := http2.ConfigureTransports(tr)
			if err != nil {
				return nil, nil, errors.New("HTTP2 configuration failed")
			}
			h.ReadIdleTimeout = 15 * time.Second
			h.PingTimeout = 15 * time.Second
		} else {
			tr.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
		}
		e = &poolEntry{tr: tr, client: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
		if len(p.entries) >= c.MaxProxyPools {
			var oldestKey string
			var oldest *poolEntry
			for k, entry := range p.entries {
				if oldest == nil || entry.used < oldest.used {
					oldestKey, oldest = k, entry
				}
			}
			oldest.retired = true
			oldest.tr.CloseIdleConnections()
			delete(p.entries, oldestKey)
		}
		p.entries[key] = e
	}
	e.used = p.tick
	e.active++
	var once sync.Once
	release := func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			e.active--
			if e.retired && e.active == 0 {
				e.tr.CloseIdleConnections()
			}
		})
	}
	return e.client, release, nil
}
func (p *transportPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, e := range p.entries {
		e.retired = true
		e.tr.CloseIdleConnections()
	}
}
func (p *transportPool) count() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.entries) }
