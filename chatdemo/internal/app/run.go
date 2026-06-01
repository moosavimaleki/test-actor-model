package app

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	ergo "ergo.services/ergo"
	"ergo.services/ergo/gen"
	"github.com/redis/go-redis/v9"

	"actor-chat-demo/internal/chat"
	"actor-chat-demo/internal/httpapi"
)

// فارسی: Run ترتیب bootstrap برنامه را مشخص می‌کند.
// فارسی: ترتیب مهم است چون actorها برای hydrate شدن به Redis نیاز دارند.
// فارسی: مسیر کلی این است: Redis -> Ergo node -> Registry actor -> HTTP/WebSocket.
func Run() error {
	// فارسی: signalCtx وقتی Ctrl+C یا SIGTERM برسد cancel می‌شود.
	// فارسی: همین context باعث می‌شود shutdown تمیز از یک نقطه مرکزی شروع شود.
	signalCtx, stopSignals := newSignalContext()
	defer stopSignals()

	// فارسی: Redis client اینجا ساخته می‌شود و به actorها تزریق می‌شود.
	// فارسی: خود actorها owner Redis client نیستند؛ فقط از آن استفاده می‌کنند.
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer closeRedis(rdb)

	// فارسی: قبل از ساخت actorها Redis را ping می‌کنیم.
	// فارسی: اگر storage آماده نباشد، hydrate کردن roomها هم معنی ندارد.
	if err := pingRedis(signalCtx, rdb); err != nil {
		return err
	}

	// فارسی: اینجا node اصلی Ergo و Registry actor ساخته می‌شوند.
	// فارسی: Registry بعداً RoomActorهای dynamic را on-demand spawn می‌کند.
	node, registryPID, err := startNode(rdb)
	if err != nil {
		return err
	}
	// فارسی: اگر هر جای Run بعد از این نقطه fail شود،
	// فارسی: node باید تمیز stop شود تا actorها orphan نمانند.
	defer node.Stop()

	// فارسی: HTTP server فقط adapter ورودی است.
	// فارسی: business logic داخل http handler نیست و به actorها پاس داده می‌شود.
	server := newHTTPServer(node, registryPID)
	serverErr := startHTTPServer(server)

	// فارسی: برنامه اینجا منتظر می‌ماند تا یا signal shutdown برسد یا server fail شود.
	if err := waitForShutdown(signalCtx, serverErr); err != nil {
		return err
	}

	// فارسی: shutdown اول HTTP را می‌بندد، بعد Registry را drain می‌کند.
	return shutdown(server, node, registryPID)
}

// فارسی: startNode runtime اصلی Ergo را می‌سازد و Registry actor را بالا می‌آورد.
// فارسی: خروجی دوم PID رجیستری است؛ از این به بعد همه requestها به همین PID route می‌شوند.
func startNode(rdb *redis.Client) (gen.Node, gen.PID, error) {
	node, err := ergo.StartNode("chat-v3@localhost", gen.NodeOptions{})
	if err != nil {
		return nil, gen.PID{}, err
	}

	// فارسی: Registry فقط lifecycle و routing را نگه می‌دارد.
	// فارسی: state هر room داخل RoomActor همان room می‌ماند.
	registryPID, err := node.Spawn(
		chat.NewRegistry,
		gen.ProcessOptions{},
		chat.RegistryConfig{Redis: rdb},
	)
	if err != nil {
		node.Stop()
		return nil, gen.PID{}, err
	}

	return node, registryPID, nil
}

// فارسی: newHTTPServer تنظیمات عملیاتی HTTP server را متمرکز می‌کند.
// فارسی: timeoutها مهم‌اند چون request کند نباید goroutine/server را بی‌نهایت نگه دارد.
func newHTTPServer(node gen.Node, registryPID gen.PID) *http.Server {
	return &http.Server{
		Addr:              ":8080",
		Handler:           httpapi.New(node, registryPID),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// فارسی: startHTTPServer سرور را در goroutine جدا اجرا می‌کند.
// فارسی: main goroutine باید آزاد بماند تا بتواند منتظر signal shutdown شود.
func startHTTPServer(server *http.Server) <-chan error {
	serverErr := make(chan error, 1)
	go func() {
		log.Println("HTTP API listening on :8080")

		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}

		serverErr <- nil
	}()
	return serverErr
}

// فارسی: pingRedis یک health check کوتاه برای Redis است.
// فارسی: timeout دو ثانیه‌ای باعث می‌شود startup روی شبکه یا Redis خراب گیر نکند.
func pingRedis(ctx context.Context, rdb *redis.Client) error {
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return rdb.Ping(pingCtx).Err()
}
