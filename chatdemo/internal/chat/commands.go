package chat

import (
	"time"

	"ergo.services/ergo/gen"
)

// فارسی: CreateRoom پیام ساخت room جدید است.
// فارسی: این پیام اول به Registry می‌رسد چون Registry مسئول lifecycle roomهاست.
type CreateRoom struct {
	RoomID string `json:"room_id"`
	Title  string `json:"title"`
}

// فارسی: JoinRoom پیام ورود کاربر به یک room است.
// فارسی: Conn فقط برای WebSocket استفاده می‌شود و داخل JSON نمی‌آید.
// فارسی: با Conn می‌فهمیم این nick متعلق به کدام connection actor زنده است.
type JoinRoom struct {
	RoomID string  `json:"room_id,omitempty"`
	Nick   string  `json:"nick"`
	Conn   gen.PID `json:"-"`
}

// فارسی: LeaveRoom پیام خروج کاربر از room است.
// فارسی: Conn کمک می‌کند socket قدیمی نتواند عضویت socket جدید را پاک کند.
type LeaveRoom struct {
	RoomID string  `json:"room_id,omitempty"`
	Nick   string  `json:"nick"`
	Conn   gen.PID `json:"-"`
}

// فارسی: PostMessage پیام ارسال chat message است.
// فارسی: اگر از WebSocket بیاید، Conn باید با عضو فعلی room یکی باشد.
type PostMessage struct {
	RoomID string  `json:"room_id,omitempty"`
	From   string  `json:"from"`
	Text   string  `json:"text"`
	Conn   gen.PID `json:"-"`
}

// فارسی: GetRoom درخواست snapshot فعلی یک room است.
// فارسی: این query از Registry عبور می‌کند تا اگر actor room خوابیده بود hydrate شود.
type GetRoom struct {
	RoomID string `json:"room_id"`
}

// فارسی: ListRooms درخواست لیست roomهای شناخته‌شده در Redis است.
type ListRooms struct{}

// فارسی: ListMessages درخواست خواندن history پیام‌های یک room است.
// فارسی: Limit جلوی برگشتن حجم زیاد داده را می‌گیرد.
type ListMessages struct {
	RoomID string `json:"room_id,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// فارسی: UnloadRoom actor یک room را پایین می‌آورد ولی Redis state را حذف نمی‌کند.
// فارسی: request بعدی همان room را دوباره از snapshot بالا می‌آورد.
type UnloadRoom struct {
	RoomID string `json:"room_id"`
}

// فارسی: DrainRegistry پیام shutdown امن است.
// فارسی: Registry با این پیام همه room actorهای فعال را snapshot و stop می‌کند.
type DrainRegistry struct{}

// فارسی: RoomSnapshot state پایدار room است که در Redis ذخیره می‌شود.
// فارسی: PID داخل snapshot نیست چون PID فقط آدرس runtime است و بعد restart معتبر نیست.
type RoomSnapshot struct {
	Version   int       `json:"version"`
	RoomID    string    `json:"room_id"`
	Title     string    `json:"title"`
	Members   []string  `json:"members"`
	UpdatedAt time.Time `json:"updated_at"`
}

// فارسی: ChatMessage مدل durable پیام chat است.
// فارسی: این struct در Redis list به شکل JSON ذخیره می‌شود.
type ChatMessage struct {
	From      string    `json:"from"`
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"created_at"`
}

// فارسی: RoomEvent پیام push داخلی از RoomActor به WSConnectionActorهاست.
// فارسی: این پیام برای broadcast زنده استفاده می‌شود، نه برای persistence.
type RoomEvent struct {
	Type    string       `json:"type"`
	RoomID  string       `json:"room_id"`
	From    string       `json:"from,omitempty"`
	Text    string       `json:"text,omitempty"`
	Members []string     `json:"members,omitempty"`
	Message *ChatMessage `json:"message,omitempty"`
}
