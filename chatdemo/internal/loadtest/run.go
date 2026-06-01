package loadtest

import (
	"fmt"
	"strings"
)

// فارسی: Run نقطه ورود loadtest است.
// فارسی: phase setup برای آماده‌سازی state و phase messages برای فشار پایدار استفاده می‌شود.
func Run() error {
	cfg := parseConfig()
	if err := validateConfig(cfg); err != nil {
		return err
	}

	client := newClient(cfg)
	if err := preflightServer(cfg, client); err != nil {
		return err
	}

	stats := newStats()

	switch cfg.Phase {
	case "setup":
		return runSetup(cfg, client, stats)
	case "messages":
		return runMessages(cfg, client, stats)
	case "all":
		if err := runSetup(cfg, client, stats); err != nil {
			return err
		}
		stats.Reset()
		return runMessages(cfg, client, stats)
	default:
		return fmt.Errorf("unknown phase %q", cfg.Phase)
	}
}

// فارسی: validateConfig جلوی اجرای تست با ورودی غیرمنطقی را می‌گیرد.
// فارسی: خطای config را باید قبل از شروع میلیون‌ها request بفهمیم.
func validateConfig(cfg Config) error {
	if cfg.Rooms <= 0 {
		return fmt.Errorf("rooms must be positive")
	}
	if cfg.UsersPerRoom <= 0 {
		return fmt.Errorf("users-per-room must be positive")
	}
	if cfg.Workers <= 0 {
		return fmt.Errorf("workers must be positive")
	}
	if cfg.RPS <= 0 {
		return fmt.Errorf("rps must be positive")
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return fmt.Errorf("base-url is required")
	}

	return nil
}
