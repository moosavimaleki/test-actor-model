package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// فارسی: hydrate state room را از Redis برمی‌گرداند.
// فارسی: این تابع دلیل اصلی durable بودن actorهای dynamic است.
func (r *RoomActor) hydrate() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// فارسی: اگر snapshot وجود نداشته باشد یعنی room از نظر دامنه وجود ندارد.
	raw, err := r.redis.Get(ctx, r.snapshotKey()).Result()
	if err != nil {
		return fmt.Errorf("room snapshot not found: %w", err)
	}

	var snap RoomSnapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		return err
	}
	// فارسی: Version برای migration آینده است.
	// فارسی: اگر ساختار snapshot عوض شد، با این فیلد می‌فهمیم چطور migrate کنیم.
	if snap.Version != 1 {
		return fmt.Errorf("unsupported room snapshot version: %d", snap.Version)
	}

	r.title = snap.Title
	r.members = make(map[string]roomMember, len(snap.Members))
	for _, nick := range snap.Members {
		nick = strings.TrimSpace(nick)
		if nick != "" {
			// فارسی: بعد از hydrate، PID اتصال نداریم چون connection runtime قبلی از بین رفته است.
			r.members[nick] = roomMember{nick: nick}
		}
	}

	return nil
}

// فارسی: snapshot state قابل ذخیره room را می‌سازد.
// فارسی: دقت کن PID و socket داخل snapshot نیستند چون runtime-only هستند.
func (r *RoomActor) snapshot() RoomSnapshot {
	return RoomSnapshot{
		Version:   1,
		RoomID:    r.roomID,
		Title:     r.title,
		Members:   r.memberList(),
		UpdatedAt: time.Now().UTC(),
	}
}

// فارسی: persistSnapshot snapshot فعلی را در Redis ذخیره می‌کند.
// فارسی: این تابع در join/leave/drain استفاده می‌شود.
func (r *RoomActor) persistSnapshot() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	payload, err := json.Marshal(r.snapshot())
	if err != nil {
		return err
	}

	return r.redis.Set(ctx, r.snapshotKey(), payload, 0).Err()
}

// فارسی: snapshotKey کلید Redis همین room برای snapshot است.
func (r *RoomActor) snapshotKey() string {
	return roomSnapshotKey(r.roomID)
}

// فارسی: messagesKey کلید Redis همین room برای message history است.
func (r *RoomActor) messagesKey() string {
	return roomMessagesKey(r.roomID)
}

// فارسی: roomSnapshotKey تابع pure برای ساختن نام کلید Redis است.
func roomSnapshotKey(roomID string) string {
	return "chat:room:" + roomID + ":snapshot"
}

// فارسی: roomMessagesKey تابع pure برای ساختن کلید history پیام‌هاست.
func roomMessagesKey(roomID string) string {
	return "chat:room:" + roomID + ":messages"
}
