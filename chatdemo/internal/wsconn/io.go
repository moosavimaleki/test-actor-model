package wsconn

import (
	"time"

	"ergo.services/ergo/gen"
)

// فارسی: readLoop JSON frameها را از socket می‌خواند.
// فارسی: این goroutine state actor را تغییر نمی‌دهد؛ فقط به mailbox actor پیام می‌فرستد.
func (a *Actor) readLoop(self gen.PID) {
	// فارسی: این limit جلوی frameهای خیلی بزرگ و مصرف بی‌حساب حافظه را می‌گیرد.
	a.conn.SetReadLimit(64 * 1024)

	for {
		var frame ClientFrame
		if err := a.conn.ReadJSON(&frame); err != nil {
			// فارسی: خطای read معمولاً یعنی socket بسته شده یا client disconnect کرده است.
			_ = a.node.Send(self, socketClosed{err: err})
			return
		}

		// فارسی: frame خوانده‌شده به actor فرستاده می‌شود تا state فقط داخل actor تغییر کند.
		_ = a.node.Send(self, inboundFrame{frame: frame})
	}
}

// فارسی: writeLoop frameهای خروجی را از channel می‌گیرد و روی socket می‌نویسد.
// فارسی: جدا بودن writeLoop باعث می‌شود actor هنگام نوشتن روی socket block نشود.
func (a *Actor) writeLoop() {
	for frame := range a.outbound {
		// فارسی: deadline باعث می‌شود client کند، write را بی‌نهایت نگه ندارد.
		_ = a.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := a.conn.WriteJSON(frame); err != nil {
			return
		}
	}
}

// فارسی: enqueue frame خروجی را بدون block طولانی وارد صف می‌کند.
// فارسی: اگر client خیلی کند باشد و buffer پر شود، فعلاً frame را drop می‌کنیم و warning می‌دهیم.
func (a *Actor) enqueue(frame ServerFrame) {
	select {
	case a.outbound <- frame:
	default:
		a.Log().Warning("websocket outbound buffer is full. conn_id=%s", a.connID)
	}
}
