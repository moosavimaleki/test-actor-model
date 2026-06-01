package chat

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"ergo.services/ergo/gen"
)

// فارسی: createRoom room جدید را به صورت durable در Redis ثبت می‌کند.
// فارسی: بعد از ساخت snapshot اولیه، actor همان room هم بالا آورده می‌شود.
func (rg *RoomRegistryActor) createRoom(req CreateRoom) Result {
	roomID := strings.TrimSpace(req.RoomID)
	title := strings.TrimSpace(req.Title)
	if roomID == "" {
		return Fail("room_id is empty")
	}
	if title == "" {
		title = roomID
	}

	// فارسی: برای Redis همیشه timeout کوتاه می‌گذاریم.
	// فارسی: actor نباید روی یک I/O خراب برای همیشه گیر کند.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// فارسی: وجود snapshot یعنی room قبلاً ساخته شده است.
	// فارسی: در این حالت فقط actor را ensure می‌کنیم و duplicate نمی‌سازیم.
	snapshotKey := roomSnapshotKey(roomID)
	exists, err := rg.redis.Exists(ctx, snapshotKey).Result()
	if err != nil {
		return Fail("redis exists failed: " + err.Error())
	}
	if exists > 0 {
		_, res := rg.ensureRoom(roomID)
		return res
	}

	// فارسی: snapshot اولیه حداقل state لازم برای hydrate شدن RoomActor است.
	snap := RoomSnapshot{
		Version:   1,
		RoomID:    roomID,
		Title:     title,
		Members:   []string{},
		UpdatedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(snap)
	if err != nil {
		return Fail("marshal snapshot failed: " + err.Error())
	}

	// فارسی: SAdd و Set را در pipeline می‌گذاریم تا ساخت room یک operation منسجم باشد.
	pipe := rg.redis.TxPipeline()
	pipe.SAdd(ctx, roomsKey(), roomID)
	pipe.Set(ctx, snapshotKey, payload, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return Fail("redis create room failed: " + err.Error())
	}

	_, res := rg.ensureRoom(roomID)
	return res
}

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
	rg.Log().Info("room actor loaded. room_id=%s pid=%s active_rooms=%d", roomID, pid, len(rg.rooms))
	return pid, OK()
}

// فارسی: roomsKey کلید Redis برای مجموعه roomIDهاست.
func roomsKey() string {
	return "chat:rooms"
}
