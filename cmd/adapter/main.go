// qqbot-adapter v2: QQ gateway -> SQLite spool -> WS replay + HTTP relay.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/elwina/qqbot-adapter/internal/config"
	"github.com/elwina/qqbot-adapter/internal/health"
	"github.com/elwina/qqbot-adapter/internal/outbox"
	"github.com/elwina/qqbot-adapter/internal/qqgw"
	"github.com/elwina/qqbot-adapter/internal/relay"
	"github.com/elwina/qqbot-adapter/internal/serve"
	"github.com/elwina/qqbot-adapter/internal/spool"
	"github.com/elwina/qqbot-adapter/internal/token"
)

// version is overridable at release time: -ldflags "-X main.version=x.y.z"
var version = "dev"

func main() {
	cfgPath := flag.String("config", "config.json", "config file path")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("qqbot-adapter", version)
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	baseDir, err := filepath.Abs(filepath.Dir(*cfgPath))
	if err != nil {
		baseDir, _ = os.Getwd()
	}
	dbPath := config.Resolve(baseDir, cfg.DBPath)
	secretsDir := config.Resolve(baseDir, cfg.SecretsDir)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		log.Fatalf("建 state 目录失败: %v", err)
	}

	sp, err := spool.Open(dbPath)
	if err != nil {
		log.Fatalf("打开 spool 失败: %v", err)
	}
	defer sp.Close()

	h := health.New()
	h.Subs = sp.SubCount

	tok := token.New(cfg.QQAPIBase, secretsDir, filepath.Dir(dbPath))
	sender := outbox.NewSender(cfg.QQAPIBase, tok)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	gw := qqgw.New(cfg.QQAPIBase, cfg.GatewayURL, cfg.Intents, sp, tok, h)
	go gw.Run(ctx)

	obw := outbox.NewWorker(sp, sender, time.Duration(cfg.OutboxMaxAgeHours)*time.Hour)
	go obw.Run(ctx)

	go retentionLoop(ctx, sp, cfg)

	wsSrv := &http.Server{
		Addr:    net.JoinHostPort(cfg.ListenIP, fmt.Sprint(cfg.WSPort)),
		Handler: serve.New(sp, h, cfg).Handler(),
	}
	relaySrv := &http.Server{
		Addr:    net.JoinHostPort(cfg.ListenIP, fmt.Sprint(cfg.RelayPort)),
		Handler: relay.New(sp, sender, h).Handler(),
	}
	go func() {
		log.Printf("[main] WS 事件服务监听 %s", wsSrv.Addr)
		if err := wsSrv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("[main] WS 服务退出: %v", err)
		}
	}()
	go func() {
		log.Printf("[main] API 中转监听 %s -> %s", relaySrv.Addr, cfg.QQAPIBase)
		if err := relaySrv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("[main] relay 服务退出: %v", err)
		}
	}()

	log.Printf("[main] qqbot-adapter v2 启动 (db=%s)", dbPath)
	<-ctx.Done()
	log.Printf("[main] 收到退出信号，关闭中")
	shutdownCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
	defer sc()
	wsSrv.Shutdown(shutdownCtx)
	relaySrv.Shutdown(shutdownCtx)
}

func retentionLoop(ctx context.Context, sp *spool.Spool, cfg *config.Config) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if cfg.RetentionDays > 0 {
			cutoff := time.Now().AddDate(0, 0, -cfg.RetentionDays).UnixMilli()
			if n, err := sp.PruneEvents(cutoff); err == nil && n > 0 {
				log.Printf("[retention] 清理 events %d 行", n)
			}
		}
		if cfg.ControlRetentionD > 0 {
			cutoff := time.Now().AddDate(0, 0, -cfg.ControlRetentionD).UnixMilli()
			sp.PruneControl(cutoff)
		}
		if cfg.OutboxKeepDays > 0 {
			cutoff := time.Now().AddDate(0, 0, -cfg.OutboxKeepDays).UnixMilli()
			sp.PruneOutbox(cutoff)
		}
	}
}
