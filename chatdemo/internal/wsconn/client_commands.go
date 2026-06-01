package wsconn

import (
	"strings"

	"actor-chat-demo/internal/chat"
)

// فارسی: handleClientFrame نقطه تبدیل protocol به command است.
// فارسی: اینجا JSON WebSocket را به JoinRoom/PostMessage و queryهای actor system تبدیل می‌کنیم.
func (a *Actor) handleClientFrame(frame ClientFrame) {
	switch strings.TrimSpace(frame.Type) {
	case "join":
		a.joinRoom(frame)
	case "leave":
		a.leaveRoom(frame)
	case "message":
		a.postMessage(frame)
	case "room":
		a.getRoom(frame)
	case "rooms":
		a.listRooms()
	case "history":
		a.history(frame)
	case "ping":
		a.enqueue(ServerFrame{Type: "pong", OK: true})
	default:
		a.enqueueFail("unknown frame type")
	}
}

// فارسی: joinRoom یک frame از client را به command JoinRoom تبدیل می‌کند.
// فارسی: PID همین actor هم پاس داده می‌شود تا RoomActor بداند این nick به کدام socket وصل است.
func (a *Actor) joinRoom(frame ClientFrame) {
	roomID := strings.TrimSpace(frame.RoomID)
	nick := strings.TrimSpace(frame.Nick)

	result, err := a.Call(a.registryPID, chat.JoinRoom{
		RoomID: roomID,
		Nick:   nick,
		Conn:   a.PID(),
	})
	if !a.writeActorResult("join_result", roomID, result, err) {
		return
	}

	// فارسی: فقط بعد از موفقیت actor، state local connection را update می‌کنیم.
	a.joined[roomID] = nick
}

// فارسی: leaveRoom از room خارج می‌شود.
// فارسی: اگر client nick نفرستد، nick همان join قبلی این connection استفاده می‌شود.
func (a *Actor) leaveRoom(frame ClientFrame) {
	roomID := strings.TrimSpace(frame.RoomID)
	nick := strings.TrimSpace(frame.Nick)
	if nick == "" {
		nick = a.joined[roomID]
	}

	result, err := a.Call(a.registryPID, chat.LeaveRoom{
		RoomID: roomID,
		Nick:   nick,
		Conn:   a.PID(),
	})
	if !a.writeActorResult("leave_result", roomID, result, err) {
		return
	}

	delete(a.joined, roomID)
}

// فارسی: postMessage پیام WebSocket را به PostMessage actor command تبدیل می‌کند.
// فارسی: اگر frame nick نداشته باشد، nick از state همین connection برداشته می‌شود.
func (a *Actor) postMessage(frame ClientFrame) {
	roomID := strings.TrimSpace(frame.RoomID)
	from := strings.TrimSpace(frame.Nick)
	if from == "" {
		from = a.joined[roomID]
	}

	result, err := a.Call(a.registryPID, chat.PostMessage{
		RoomID: roomID,
		From:   from,
		Text:   frame.Text,
		Conn:   a.PID(),
	})
	a.writeActorResult("message_result", roomID, result, err)
}
