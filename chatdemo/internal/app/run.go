package app

import (
	"github.com/redis/go-redis/v9"
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
