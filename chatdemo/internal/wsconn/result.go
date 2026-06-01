package wsconn

import "actor-chat-demo/internal/chat"

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

func (a *Actor) enqueueFail(message string) {
	a.enqueue(ServerFrame{Type: "error", Error: message})
}
