package wsconn

import "actor-chat-demo/internal/chat"

// HandleMessage در فایل actor.go پیام‌های داخلی را هندل می‌کند.
// این method جدا برای روشن کردن مسیر eventهای RoomActor است.
func (a *Actor) handleRoomEvent(event chat.RoomEvent) {
	a.enqueue(ServerFrame{
		Type:  "room_event",
		Event: &event,
	})
}
