package chat

import (
	"context"
	"encoding/json"
	"strings"
	"time"
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
