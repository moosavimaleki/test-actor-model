package wsconn

import (
	"strings"

	"actor-chat-demo/internal/chat"
)

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

	a.joined[roomID] = nick
}

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
