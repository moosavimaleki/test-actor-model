package wsconn

import (
	"time"

	"ergo.services/ergo/gen"
)

func (a *Actor) readLoop(self gen.PID) {
	a.conn.SetReadLimit(64 * 1024)

	for {
		var frame ClientFrame
		if err := a.conn.ReadJSON(&frame); err != nil {
			_ = a.node.Send(self, socketClosed{err: err})
			return
		}

		_ = a.node.Send(self, inboundFrame{frame: frame})
	}
}

func (a *Actor) writeLoop() {
	for frame := range a.outbound {
		_ = a.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := a.conn.WriteJSON(frame); err != nil {
			return
		}
	}
}

func (a *Actor) enqueue(frame ServerFrame) {
	select {
	case a.outbound <- frame:
	default:
		a.Log().Warning("websocket outbound buffer is full. conn_id=%s", a.connID)
	}
}
