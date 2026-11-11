package timing

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRetiredPoolClosesConnectionAfterRequestCompletes(t *testing.T) {
	closed := make(chan struct{}, 2)
	up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "real output")
	}))
	up.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	up.Start()
	defer up.Close()
	cfg := Defaults()
	cfg.MaxProxyPools = 1
	p := newPool(cfg)
	defer p.close()
	client, release, err := p.acquire("", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", up.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, release2, err := p.acquire("", 2)
	if err != nil {
		t.Fatal(err)
	}
	release2()
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(body) != "real output" {
		t.Fatal("eviction interrupted active response", err)
	}
	release()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("retired pool retained idle connection")
	}
}

func TestTransportUsesConfiguredHTTPProxy(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "http://fixture.invalid/v1/responses" || r.Host != "fixture.invalid" {
			t.Error("proxy request target changed", r.URL, r.Host)
		}
		io.WriteString(w, "proxy output")
	}))
	defer proxy.Close()
	p := newPool(Defaults())
	defer p.close()
	client, release, err := p.acquire(proxy.URL, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://fixture.invalid/v1/responses", strings.NewReader("{}"))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "proxy output" {
		t.Fatal("configured proxy not used", err)
	}
}
