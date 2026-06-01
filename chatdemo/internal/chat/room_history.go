package chat

import (
	"context"
	"encoding/json"
	"time"
)

// فارسی: listMessages history را از Redis می‌خواند، نه از RAM actor.
// فارسی: message history durable است و با unload شدن actor از بین نمی‌رود.
func (r *RoomActor) listMessages(limit int) any {
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	// فارسی: LRANGE با index منفی یعنی از انتهای list بخوان.
	// فارسی: چون با RPUSH اضافه می‌کنیم، آخر list جدیدترین پیام‌هاست.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rows, err := r.redis.LRange(ctx, r.messagesKey(), int64(-limit), -1).Result()
	if err != nil {
		return Fail("redis LRANGE failed: " + err.Error())
	}

	messages := make([]ChatMessage, 0, len(rows))
	for _, row := range rows {
		var msg ChatMessage
		// فارسی: هر row یک JSON ذخیره‌شده در Redis است و باید به struct Go تبدیل شود.
		if err := json.Unmarshal([]byte(row), &msg); err != nil {
			return Fail("unmarshal message failed: " + err.Error())
		}
		messages = append(messages, msg)
	}

	return map[string]any{
		"room_id":  r.roomID,
		"messages": messages,
	}
}
