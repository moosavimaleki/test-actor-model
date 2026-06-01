package loadtest

import (
	"context"
	"log"
	"time"
)

// فارسی: reportLoop در فاصله‌های ثابت وضعیت loadtest را log می‌کند.
// فارسی: خروجی لحظه‌ای کمک می‌کند بفهمی تست به target RPS نزدیک شده یا bottleneck خورده است.
func reportLoop(ctx context.Context, phase string, every time.Duration, stats *Stats, done <-chan struct{}) {
	if every <= 0 {
		return
	}

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			log.Printf("%s %s", phase, stats.Snapshot().String())
		}
	}
}
