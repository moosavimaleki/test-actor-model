package wsconn

import (
	"fmt"

	"github.com/gorilla/websocket"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"

	"actor-chat-demo/internal/chat"
)

// Actor نماینده یک WebSocket connection زنده است.
//
// این actor مالک state اتصال است: nick و roomهایی که همین socket join کرده.
// goroutineهای read/write فقط I/O انجام می‌دهند و state actor را مستقیم تغییر نمی‌دهند.
type Actor struct {
	act.Actor

	node        gen.Node
	registryPID gen.PID
	connID      string
	conn        *websocket.Conn
	outbound    chan ServerFrame
	joined      map[string]string
}

func New() gen.ProcessBehavior {
	return &Actor{}
}

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
	go a.readLoop(self)
	go a.writeLoop()

	a.enqueue(ServerFrame{Type: "welcome", OK: true, ConnID: a.connID})
	a.Log().Info("websocket actor started. conn_id=%s pid=%s", a.connID, self)
	return nil
}

func (a *Actor) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case inboundFrame:
		a.handleClientFrame(msg.frame)
	case chat.RoomEvent:
		a.handleRoomEvent(msg)
	case socketClosed:
		a.leaveJoinedRooms()
		return gen.TerminateReasonNormal
	default:
		a.Log().Warning("unknown websocket actor message: %#v", message)
	}

	return nil
}

func (a *Actor) Terminate(reason error) {
	close(a.outbound)
	_ = a.conn.Close()
	a.Log().Info("websocket actor stopped. conn_id=%s reason=%v", a.connID, reason)
}
