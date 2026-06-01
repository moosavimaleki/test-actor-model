package wsconn

import "actor-chat-demo/internal/chat"

func (a *Actor) getRoom(frame ClientFrame) {
	result, err := a.Call(a.registryPID, chat.GetRoom{RoomID: frame.RoomID})
	a.writeActorResult("room", frame.RoomID, result, err)
}

func (a *Actor) listRooms() {
	result, err := a.Call(a.registryPID, chat.ListRooms{})
	a.writeActorResult("rooms", "", result, err)
}

func (a *Actor) history(frame ClientFrame) {
	result, err := a.Call(a.registryPID, chat.ListMessages{
		RoomID: frame.RoomID,
		Limit:  frame.Limit,
	})
	a.writeActorResult("history", frame.RoomID, result, err)
}

func (a *Actor) leaveJoinedRooms() {
	for roomID, nick := range a.joined {
		_, _ = a.Call(a.registryPID, chat.LeaveRoom{
			RoomID: roomID,
			Nick:   nick,
			Conn:   a.PID(),
		})
	}
}
