package wsconn

import (
	"fmt"

	"github.com/gorilla/websocket"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"

	"actor-chat-demo/internal/chat"
)

// فارسی: Actor نماینده یک WebSocket connection زنده است.
// فارسی: این actor مالک state اتصال است: nick و roomهایی که همین socket join کرده.
// فارسی: goroutineهای read/write فقط I/O انجام می‌دهند و state actor را مستقیم تغییر نمی‌دهند.
type Actor struct {
	act.Actor

	// فارسی: node برای Send کردن پیام داخلی از goroutine خواندن socket به خود actor استفاده می‌شود.
	node gen.Node
	// فارسی: registryPID برای ارسال commandهای join/message/history به actor system لازم است.
	registryPID gen.PID
	// فارسی: connID شناسه debug اتصال است.
	connID string
	// فارسی: conn همان WebSocket خام است؛ دسترسی مستقیم به آن محدود به این actor است.
	conn *websocket.Conn
	// فارسی: outbound صف خروجی frameهایی است که باید روی socket نوشته شوند.
	outbound chan ServerFrame
	// فارسی: joined نگه می‌دارد این socket با چه nickی در چه roomهایی join کرده است.
	joined map[string]string
}

// فارسی: New factory مورد نیاز Ergo برای ساخت WSConnectionActor است.
func New() gen.ProcessBehavior {
	return &Actor{}
}

// فارسی: Init هنگام spawn شدن connection actor اجرا می‌شود.
// فارسی: اینجا socket loopها شروع می‌شوند و frame welcome به client فرستاده می‌شود.
func (a *Actor) Init(args ...any) error {
	if len(args) != 1 {
		return fmt.Errorf("WSConnectionActor needs Config")
	}

	cfg, ok := args[0].(Config)
	if !ok {
		return fmt.Errorf("invalid ws config: %T", args[0])
	}

	a.node = cfg.Node
	a.registryPID = cfg.RegistryPID
	a.connID = cfg.ConnID
	a.conn = cfg.Conn
	a.outbound = make(chan ServerFrame, 32)
	a.joined = make(map[string]string)

	self := a.PID()
	// فارسی: readLoop و writeLoop I/O انجام می‌دهند، ولی state actor را مستقیم تغییر نمی‌دهند.
	go a.readLoop(self)
	go a.writeLoop()

	a.enqueue(ServerFrame{Type: "welcome", OK: true, ConnID: a.connID})
	a.Log().Info("websocket actor started. conn_id=%s pid=%s", a.connID, self)
	return nil
}

// فارسی: HandleMessage همه پیام‌های async این connection actor را هندل می‌کند.
// فارسی: پیام‌ها شامل frame ورودی، event از RoomActor و بسته شدن socket هستند.
func (a *Actor) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case inboundFrame:
		// فارسی: frame خام client اینجا به command دامنه تبدیل می‌شود.
		a.handleClientFrame(msg.frame)
	case chat.RoomEvent:
		// فارسی: RoomActor برای broadcast زنده، RoomEvent را به این actor Send می‌کند.
		a.handleRoomEvent(msg)
	case socketClosed:
		// فارسی: با بسته شدن socket، actor باید از roomهایی که join کرده leave کند.
		a.leaveJoinedRooms()
		return gen.TerminateReasonNormal
	default:
		a.Log().Warning("unknown websocket actor message: %#v", message)
	}

	return nil
}

// فارسی: Terminate آخر عمر connection actor است.
// فارسی: اینجا channel خروجی و خود WebSocket را می‌بندیم.
func (a *Actor) Terminate(reason error) {
	close(a.outbound)
	_ = a.conn.Close()
	a.Log().Info("websocket actor stopped. conn_id=%s reason=%v", a.connID, reason)
}
