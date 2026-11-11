package timing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
)

const PluginID = "inventory.openai.stream-timing"
const Version = "0.1.4-am1.1"
const Capability = "openai.oauth.outbound_transport.v1"
const APIKeyCapability = "openai.apikey.outbound_transport.v1"

type Config struct {
	RetryMaxAttempts   int `json:"retry_max_attempts"`
	RetryBodyMaxBytes  int `json:"retry_body_max_bytes"`
	RetryMaxConcurrent int `json:"retry_max_concurrent"`
	RetryBackoffMS     int `json:"retry_backoff_ms"`
	RetryTotalWaitMS   int `json:"retry_total_wait_ms"`

	Mode                         string `json:"mode"`
	DelayMS                      int    `json:"delay_ms"`
	HintAfterMS                  int    `json:"hint_after_ms"`
	HintWaitMode                 string `json:"hint_wait_mode"`
	HintMinMS                    int    `json:"hint_min_ms"`
	HintMaxMS                    int    `json:"hint_max_ms"`
	MaxEventBytes                int    `json:"max_event_bytes"`
	MaxConcurrentTransforms      int    `json:"max_concurrent_transforms"`
	MaxProxyPools                int    `json:"max_proxy_pools"`
	IdleConnectionsPerPool       int    `json:"idle_connections_per_pool"`
	IdleTimeoutSeconds           int    `json:"idle_timeout_seconds"`
	DialTimeoutSeconds           int    `json:"dial_timeout_seconds"`
	TLSHandshakeTimeoutSeconds   int    `json:"tls_handshake_timeout_seconds"`
	ResponseHeaderTimeoutSeconds int    `json:"response_header_timeout_seconds"`
	EnableHTTP2                  bool   `json:"enable_http2"`
}

func Defaults() Config {
	return Config{Mode: "off", DelayMS: 1000, HintAfterMS: 1000, HintWaitMode: "fixed", HintMinMS: 1000, HintMaxMS: 2000, MaxEventBytes: 262144, MaxConcurrentTransforms: 128, MaxProxyPools: 64, IdleConnectionsPerPool: 8, IdleTimeoutSeconds: 90, DialTimeoutSeconds: 30, TLSHandshakeTimeoutSeconds: 10, EnableHTTP2: true, RetryMaxAttempts: 2, RetryBodyMaxBytes: 2097152, RetryMaxConcurrent: 32, RetryBackoffMS: 500, RetryTotalWaitMS: 10000}
}

func (c Config) timedHint() bool {
	return c.Mode == "hint" && (c.HintWaitMode == "random" || c.HintAfterMS > 0)
}

// Each admitted request samples once. rand/v2's package functions are safe for
// concurrent callers; there is no per-request time seed or shared local RNG.
func (c Config) hintThresholdMS() int {
	if c.HintWaitMode == "random" {
		return c.HintMinMS + rand.IntN(c.HintMaxMS-c.HintMinMS+1)
	}
	return c.HintAfterMS
}

func ParseConfig(b []byte) (Config, error) {
	c := Defaults()
	if len(bytes.TrimSpace(b)) == 0 {
		return c, nil
	}
	if len(b) > 16384 || bytes.TrimSpace(b)[0] != '{' {
		return c, fmt.Errorf("config must be a JSON object, at most 16384 bytes")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return c, fmt.Errorf("invalid configuration JSON")
	}
	for _, v := range fields {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return c, fmt.Errorf("configuration fields must not be null")
		}
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, fmt.Errorf("invalid configuration: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("configuration must contain exactly one object")
	}
	if c.Mode != "off" && c.Mode != "hint" && c.Mode != "delay" {
		return c, fmt.Errorf("mode must be off, hint or delay")
	}
	if c.HintWaitMode != "fixed" && c.HintWaitMode != "random" {
		return c, fmt.Errorf("hint_wait_mode must be fixed or random")
	}
	limits := []struct {
		name            string
		value, min, max int
	}{
		{"retry_max_attempts", c.RetryMaxAttempts, 0, 3},
		{"retry_body_max_bytes", c.RetryBodyMaxBytes, 65536, 8388608},
		{"retry_max_concurrent", c.RetryMaxConcurrent, 1, 128},
		{"retry_backoff_ms", c.RetryBackoffMS, 1, 10000},
		{"retry_total_wait_ms", c.RetryTotalWaitMS, 1, 60000},
		{"hint_after_ms", c.HintAfterMS, 0, 60000},
		{"hint_min_ms", c.HintMinMS, 1, 60000}, {"hint_max_ms", c.HintMaxMS, 1, 60000},
		{"delay_ms", c.DelayMS, 0, 30000}, {"max_event_bytes", c.MaxEventBytes, 4096, 1048576},
		{"max_concurrent_transforms", c.MaxConcurrentTransforms, 1, 4096}, {"max_proxy_pools", c.MaxProxyPools, 1, 512},
		{"idle_connections_per_pool", c.IdleConnectionsPerPool, 1, 64}, {"idle_timeout_seconds", c.IdleTimeoutSeconds, 1, 600},
		{"dial_timeout_seconds", c.DialTimeoutSeconds, 1, 120}, {"tls_handshake_timeout_seconds", c.TLSHandshakeTimeoutSeconds, 1, 60},
		{"response_header_timeout_seconds", c.ResponseHeaderTimeoutSeconds, 0, 600},
	}
	for _, l := range limits {
		if l.value < l.min || l.value > l.max {
			return c, fmt.Errorf("%s must be between %d and %d", l.name, l.min, l.max)
		}
	}
	if c.HintMinMS > c.HintMaxMS {
		return c, fmt.Errorf("hint_min_ms must not exceed hint_max_ms")
	}
	return c, nil
}
