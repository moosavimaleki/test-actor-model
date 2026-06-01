package chat

import (
	"context"
	"strings"
	"time"

	"ergo.services/ergo/gen"
)

// فارسی: ensureRoom قلب dynamic actor system است.
// فارسی: اگر room actor فعال نباشد ولی snapshot در Redis باشد،
// فارسی: همین‌جا actor تازه ساخته و از Redis hydrate می‌شود.
func (rg *RoomRegistryActor) ensureRoom(roomID string) (gen.PID, Result) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return gen.PID{}, Fail("room_id is empty")
	}

	// فارسی: اگر actor همین الان فعال است، همان PID runtime را برمی‌گردانیم.
	if pid, exists := rg.rooms[roomID]; exists {
		return pid, OK()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	exists, err := rg.redis.Exists(ctx, roomSnapshotKey(roomID)).Result()
	if err != nil {
		return gen.PID{}, Fail("redis exists failed: " + err.Error())
	}
	if exists == 0 {
		return gen.PID{}, Fail("room not found")
	}

	// فارسی: این Spawn یک RoomActor مستقل برای همین roomID می‌سازد.
	// فارسی: RoomActor در Init خودش snapshot را از Redis می‌خواند.
	pid, err := rg.Spawn(
		NewRoom,
		gen.ProcessOptions{},
		RoomConfig{RoomID: roomID, Redis: rg.redis},
	)
	if err != nil {
		return gen.PID{}, Fail("spawn room failed: " + err.Error())
	}

	rg.rooms[roomID] = pid
	if verboseActorLogs() {
		rg.Log().Info("room actor loaded. room_id=%s pid=%s active_rooms=%d", roomID, pid, len(rg.rooms))
	}
	return pid, OK()
}

// فارسی: roomsKey کلید Redis برای مجموعه roomIDهاست.
func roomsKey() string {
	return "chat:rooms"
}
