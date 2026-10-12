// Package token fetches and caches the QQ bot access_token.
//
// Mirrors v1 qqbot_auth.py: secrets from secrets/*.txt (0600), disk cache in
// state/token_cache.json, refresh 600s before expiry.
package token

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const refreshMargin = 600 * time.Second

type Provider struct {
	apiBase    string
	secretsDir string
	cacheFile  string
	client     *http.Client

	mu        sync.Mutex
	cached    string
	expiresAt time.Time
}

func New(apiBase, secretsDir, stateDir string) *Provider {
	return &Provider{
		apiBase:    strings.TrimRight(apiBase, "/"),
		secretsDir: secretsDir,
		cacheFile:  filepath.Join(stateDir, "token_cache.json"),
		client:     &http.Client{Timeout: 15 * time.Second},
	}
}

func (p *Provider) readSecret(name string) (string, error) {
	b, err := os.ReadFile(filepath.Join(p.secretsDir, name+".txt"))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// Token returns a valid access_token, refreshing when within the margin.
func (p *Provider) Token() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cached != "" && time.Until(p.expiresAt) > refreshMargin {
		return p.cached, nil
	}
	// Disk cache survives restarts (v1-compatible token_cache.json).
	if t, exp, ok := p.readCache(); ok && time.Until(exp) > refreshMargin {
		p.cached, p.expiresAt = t, exp
		return t, nil
	}
	t, ttl, err := p.request()
	if err != nil {
		return "", err
	}
	p.cached = t
	p.expiresAt = time.Now().Add(ttl)
	p.writeCache(t, p.expiresAt)
	return t, nil
}

func (p *Provider) readCache() (string, time.Time, bool) {
	b, err := os.ReadFile(p.cacheFile)
	if err != nil {
		return "", time.Time{}, false
	}
	var c struct {
		AccessToken string  `json:"access_token"`
		ExpiresAt   float64 `json:"expires_at"`
	}
	if json.Unmarshal(b, &c) != nil || c.AccessToken == "" {
		return "", time.Time{}, false
	}
	return c.AccessToken, time.UnixMilli(int64(c.ExpiresAt * 1000)), true
}

func (p *Provider) writeCache(t string, exp time.Time) {
	if dir := filepath.Dir(p.cacheFile); dir != "" {
		os.MkdirAll(dir, 0o700)
	}
	b, _ := json.Marshal(map[string]any{
		"access_token": t,
		"expires_at":   float64(exp.UnixMilli()) / 1000.0,
	})
	tmp := p.cacheFile + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		os.Rename(tmp, p.cacheFile)
	}
}

// request calls POST {apiBase}/app/getAppAccessToken.
func (p *Provider) request() (string, time.Duration, error) {
	appID, err := p.readSecret("appid")
	if err != nil {
		return "", 0, fmt.Errorf("读 appid: %w", err)
	}
	secret, err := p.readSecret("app_secret")
	if err != nil {
		return "", 0, fmt.Errorf("读 app_secret: %w", err)
	}
	body, _ := json.Marshal(map[string]string{"appId": appID, "clientSecret": secret})
	resp, err := p.client.Post(p.apiBase+"/app/getAppAccessToken", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", 0, fmt.Errorf("取 token 网络失败: %w", err)
	}
	defer resp.Body.Close()
	var data struct {
		AccessToken string      `json:"access_token"`
		ExpiresIn   json.Number `json:"expires_in"` // QQ 实测返回字符串 "7200"
	}
	if resp.StatusCode/100 != 2 {
		return "", 0, fmt.Errorf("取 token 接口 HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", 0, fmt.Errorf("取 token 返回解析失败: %w", err)
	}
	if data.AccessToken == "" {
		return "", 0, fmt.Errorf("取 token 返回中无 access_token")
	}
	expiresIn, err := data.ExpiresIn.Int64()
	if err != nil || expiresIn <= 0 {
		expiresIn = 7200
	}
	return data.AccessToken, time.Duration(expiresIn) * time.Second, nil
}
