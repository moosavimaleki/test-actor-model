package wsconn

import "actor-chat-demo/internal/chat"

// فارسی: getRoom snapshot فعلی room را از actor system می‌گیرد.
func (a *Actor) getRoom(frame ClientFrame) {
	result, err := a.Call(a.registryPID, chat.GetRoom{RoomID: frame.RoomID})
	a.writeActorResult("room", frame.RoomID, result, err)
}

// فارسی: listRooms لیست roomها را از Registry می‌گیرد.
func (a *Actor) listRooms() {
	result, err := a.Call(a.registryPID, chat.ListRooms{})
	a.writeActorResult("rooms", "", result, err)
}

// فارسی: history پیام‌های اخیر room را از مسیر RoomActor/Redis می‌خواند.
func (a *Actor) history(frame ClientFrame) {
	result, err := a.Call(a.registryPID, chat.ListMessages{
		RoomID: frame.RoomID,
		Limit:  frame.Limit,
	})
	a.writeActorResult("history", frame.RoomID, result, err)
}

// فارسی: leaveJoinedRooms هنگام disconnect اجرا می‌شود.
// فارسی: این کار presence را تمیز می‌کند تا member قطع‌شده در room نماند.
func (a *Actor) leaveJoinedRooms() {
	for roomID, nick := range a.joined {
		_, _ = a.Call(a.registryPID, chat.LeaveRoom{
			RoomID: roomID,
			Nick:   nick,
			Conn:   a.PID(),
		})
	}
}
