package wsconn

import (
	"github.com/gorilla/websocket"

	"ergo.services/ergo/gen"
)

// فارسی: Config وابستگی‌های لازم برای ساخت WSConnectionActor است.
// فارسی: HTTP handler بعد از upgrade شدن socket این config را به actor می‌دهد.
type Config struct {
	// فارسی: Node برای Send کردن پیام از readLoop به mailbox همین actor لازم است.
	Node gen.Node
	// فارسی: RegistryPID آدرس actor رجیستری است؛ connection actor commandها را به آن می‌فرستد.
	RegistryPID gen.PID
	// فارسی: ConnID فقط برای log و debug است.
	ConnID string
	// فارسی: Conn خود اتصال WebSocket است و فقط داخل WSConnectionActor مدیریت می‌شود.
	Conn *websocket.Conn
}
