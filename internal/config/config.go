// Package config loads the adapter JSON configuration.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Config struct {
	ListenIP          string `json:"listen_ip"`
	WSPort            int    `json:"ws_port"`
	RelayPort         int    `json:"relay_port"`
	DBPath            string `json:"db_path"`
	Intents           int64  `json:"intents"`
	RetentionDays     int    `json:"retention_days"`
	ControlRetentionD int    `json:"control_retention_days"`
	OutboxMaxAgeHours int    `json:"outbox_max_age_hours"`
	OutboxKeepDays    int    `json:"outbox_keep_days"`
	ReplayBatch       int    `json:"replay_batch"`
	SecretsDir        string `json:"secrets_dir"`

	// Test/deployment hooks (not normally set).
	QQAPIBase  string `json:"qq_api_base"` // override https://api.bot.qq.com
	GatewayURL string `json:"gateway_url"` // skip /gateway/bot, dial this directly
}

func defaults() Config {
	return Config{
		ListenIP:          "0.0.0.0",
		WSPort:            8765,
		RelayPort:         8766,
		DBPath:            "state/adapter.db",
		Intents:           50331648, // 1<<24 | 1<<25
		RetentionDays:     30,
		ControlRetentionD: 7,
		OutboxMaxAgeHours: 24,
		OutboxKeepDays:    7,
		ReplayBatch:       500,
		SecretsDir:        "secrets",
		QQAPIBase:         "https://api.bot.qq.com",
	}
}

// Load reads path; a missing file yields defaults (paths stay relative to CWD).
func Load(path string) (*Config, error) {
	cfg := defaults()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &cfg, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &cfg, nil
}

// Resolve turns a possibly-relative path into absolute, rooted at dir.
func Resolve(dir, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(dir, p)
}
