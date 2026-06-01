package wsconn

import "actor-chat-demo/internal/chat"

// فارسی: handleRoomEvent مسیر eventهای RoomActor به WebSocket است.
// فارسی: این method جدا شده تا معلوم باشد push زنده از commandهای client جداست.
func (a *Actor) handleRoomEvent(event chat.RoomEvent) {
	a.enqueue(ServerFrame{
		Type:  "room_event",
		Event: &event,
	})
}
