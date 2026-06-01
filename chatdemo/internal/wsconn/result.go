package wsconn

import "actor-chat-demo/internal/chat"

// فارسی: writeActorResult جواب actorها را به ServerFrame تبدیل می‌کند.
// فارسی: این معادل WebSocket نسخه writeActorResult در HTTP adapter است.
func (a *Actor) writeActorResult(frameType, roomID string, result any, err error) bool {
	if err != nil {
		a.enqueue(ServerFrame{Type: frameType, RoomID: roomID, Error: err.Error()})
		return false
	}

	if res, ok := result.(chat.Result); ok {
		if !res.OK {
			a.enqueue(ServerFrame{Type: frameType, RoomID: roomID, Error: res.Error})
			return false
		}

		a.enqueue(ServerFrame{Type: frameType, OK: true, RoomID: roomID, Result: res})
		return true
	}

	a.enqueue(ServerFrame{Type: frameType, OK: true, RoomID: roomID, Result: result})
	return true
}

// فارسی: enqueueFail یک error frame ساده برای protocol error می‌سازد.
func (a *Actor) enqueueFail(message string) {
	a.enqueue(ServerFrame{Type: "error", Error: message})
}
