package chat

import "os"

// فارسی: verboseActorLogs لاگ‌های پرحجم مربوط به هر room actor را کنترل می‌کند.
// فارسی: برای load test بزرگ، لاگ per-room خودش bottleneck می‌شود و باید خاموش باشد.
func verboseActorLogs() bool {
	return os.Getenv("CHAT_VERBOSE_ACTORS") == "1"
}
