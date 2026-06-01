package app

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ergo.services/ergo/gen"
	"github.com/redis/go-redis/v9"

	"actor-chat-demo/internal/chat"
)

const defaultShutdownTimeout = 10 * time.Second

// فارسی: newSignalContext contextای می‌سازد که با Ctrl+C یا SIGTERM cancel می‌شود.
// فارسی: SIGTERM همان signal رایج shutdown در container و systemd است.
func newSignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
}

// فارسی: waitForShutdown دو مسیر توقف را یکی می‌کند.
// فارسی: یا خودمان signal می‌گیریم، یا HTTP server با خطای واقعی می‌خوابد.
func waitForShutdown(signalCtx context.Context, serverErr <-chan error) error {
	select {
	case <-signalCtx.Done():
		log.Println("shutdown signal received")
		return nil
	case err := <-serverErr:
		return err
	}
}

// فارسی: shutdown ترتیب deploy امن را نگه می‌دارد.
// فارسی: اول HTTP بسته می‌شود تا request جدید وارد نشود.
// فارسی: بعد Registry همه RoomActorهای فعال را drain می‌کند.
func shutdown(server *http.Server, node gen.Node, registryPID gen.PID) error {
	// فارسی: برای Shutdown از context تازه استفاده می‌کنیم.
	// فارسی: signalCtx قبلی cancel شده و برای کار جدید مناسب نیست.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout())
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("http graceful shutdown failed: %v", err)
	} else {
		log.Println("http server stopped")
	}

	// فارسی: drain یعنی قبل از stop شدن actorها snapshot نهایی در Redis نوشته شود.
	// فارسی: اگر این مرحله را حذف کنیم، آخرین تغییرات ممکن است فقط در RAM actor بمانند.
	result, err := node.Call(registryPID, chat.DrainRegistry{})
	if err != nil {
		log.Printf("registry drain failed: %v", err)
	} else if res, ok := result.(chat.Result); ok && !res.OK {
		log.Printf("registry drain failed: %s", res.Error)
	} else {
		log.Println("registry drained")
	}

	log.Println("shutdown complete")
	return nil
}

// فارسی: shutdownTimeout زمان مجاز برای HTTP shutdown و Registry drain را می‌دهد.
// فارسی: برای تست ۱ میلیون actor، مقدار پیش‌فرض ۱۰ ثانیه کم است؛ مثلا CHAT_SHUTDOWN_TIMEOUT=5m بده.
func shutdownTimeout() time.Duration {
	raw := os.Getenv("CHAT_SHUTDOWN_TIMEOUT")
	if raw == "" {
		return defaultShutdownTimeout
	}

	timeout, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("invalid CHAT_SHUTDOWN_TIMEOUT=%q, using %s", raw, defaultShutdownTimeout)
		return defaultShutdownTimeout
	}

	return timeout
}

// فارسی: closeRedis آخرین مرحله shutdown است.
// فارسی: Redis را بعد از drain می‌بندیم، نه قبل از آن.
func closeRedis(rdb *redis.Client) {
	if err := rdb.Close(); err != nil {
		log.Printf("redis close failed: %v", err)
		return
	}

	log.Println("redis client closed")
}
