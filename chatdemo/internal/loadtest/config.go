package loadtest

import (
	"flag"
	"time"
)

type Config struct {
	BaseURL      string
	Phase        string
	RoomStart    int
	Rooms        int
	UsersPerRoom int
	Workers      int
	RPS          int
	Duration     time.Duration
	Timeout      time.Duration
	ReportEvery  time.Duration
	Preflight    bool
	AutoSetup    bool
	ServerCheck  bool
}

// فارسی: parseConfig همه flagهای loadtest را می‌خواند.
// فارسی: با room-start و rooms می‌توانی تست را بین چند ماشین shard کنی.
func parseConfig() Config {
	cfg := Config{}

	flag.StringVar(&cfg.BaseURL, "base-url", "http://localhost:8080", "chat service base URL")
	flag.StringVar(&cfg.Phase, "phase", "all", "setup, messages, or all")
	flag.IntVar(&cfg.RoomStart, "room-start", 0, "first numeric room id for this load generator")
	flag.IntVar(&cfg.Rooms, "rooms", 1000, "number of rooms handled by this load generator")
	flag.IntVar(&cfg.UsersPerRoom, "users-per-room", 2, "members to join per room")
	flag.IntVar(&cfg.Workers, "workers", 256, "concurrent HTTP workers")
	flag.IntVar(&cfg.RPS, "rps", 1000, "target message requests per second")
	flag.DurationVar(&cfg.Duration, "duration", time.Minute, "message phase duration")
	flag.DurationVar(&cfg.Timeout, "timeout", 5*time.Second, "per-request timeout")
	flag.DurationVar(&cfg.ReportEvery, "report-every", 5*time.Second, "stats report interval")
	flag.BoolVar(&cfg.Preflight, "preflight", true, "check first room/member before message phase")
	flag.BoolVar(&cfg.AutoSetup, "auto-setup", true, "create/join missing rooms during message phase")
	flag.BoolVar(&cfg.ServerCheck, "server-check", true, "check /healthz before sending load")
	flag.Parse()

	return cfg
}
