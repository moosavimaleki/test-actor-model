package app

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// فارسی: pingRedis یک health check کوتاه برای Redis است.
// فارسی: timeout دو ثانیه‌ای باعث می‌شود startup روی شبکه یا Redis خراب گیر نکند.
func pingRedis(ctx context.Context, rdb *redis.Client) error {
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return rdb.Ping(pingCtx).Err()
}
