package chat

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"ergo.services/ergo/gen"
)

// فارسی: postMessage ruleهای ارسال پیام را enforce و سپس پیام را persist می‌کند.
// فارسی: فقط member فعلی room اجازه ارسال پیام دارد.
func (r *RoomActor) postMessage(from, text string, conn gen.PID) Result {
	from = strings.TrimSpace(from)
	text = strings.TrimSpace(text)

	if from == "" {
		return Fail("from is empty")
	}
	if text == "" {
		return Fail("text is empty")
	}
	member, exists := r.members[from]
	if !exists {
		return Fail("sender is not member of room")
	}
	// فارسی: اگر پیام از WebSocket آمده باشد، اتصال باید همان اتصال فعلی member باشد.
	// فارسی: این جلوی سوءاستفاده socket قدیمی یا nick تقلبی را می‌گیرد.
	if conn != (gen.PID{}) && member.pid != conn {
		return Fail("sender connection is not current member")
	}

	// فارسی: زمان پیام را داخل actor می‌سازیم تا client نتواند timestamp جعلی بدهد.
	msg := ChatMessage{
		From:      from,
		Text:      text,
		CreatedAt: time.Now().UTC(),
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return Fail("marshal message failed: " + err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// فارسی: پیام و snapshot در یک Redis transaction pipeline نوشته می‌شوند.
	// فارسی: این transaction مثل SQL کامل نیست، ولی commandها را پشت سر هم و منسجم می‌فرستد.
	pipe := r.redis.TxPipeline()
	pipe.RPush(ctx, r.messagesKey(), payload)
	// فارسی: فقط آخرین ۱۰۰ پیام را نگه می‌داریم تا Redis list بی‌نهایت رشد نکند.
	pipe.LTrim(ctx, r.messagesKey(), -100, -1)

	// فارسی: snapshot هم بعد از message update می‌شود تا UpdatedAt و members durable بمانند.
	snapshotPayload, err := json.Marshal(r.snapshot())
	if err != nil {
		return Fail("marshal snapshot failed: " + err.Error())
	}
	pipe.Set(ctx, r.snapshotKey(), snapshotPayload, 0)

	if _, err := pipe.Exec(ctx); err != nil {
		return Fail("redis write failed: " + err.Error())
	}

	// فارسی: broadcast فقط بعد از موفقیت Redis انجام می‌شود.
	// فارسی: اگر persistence fail شود، clientها event اشتباه نمی‌گیرند.
	r.broadcast(RoomEvent{
		Type:    "message",
		RoomID:  r.roomID,
		From:    from,
		Text:    text,
		Members: r.memberList(),
		Message: &msg,
	})
	return OK()
}
