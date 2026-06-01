package wsconn

import "actor-chat-demo/internal/chat"

// فارسی: ClientFrame فرم JSON پیام‌هایی است که browser/client به WebSocket می‌فرستد.
// فارسی: فیلد Type مشخص می‌کند frame قرار است join، message، history یا چیز دیگر باشد.
type ClientFrame struct {
	Type   string `json:"type"`
	RoomID string `json:"room_id,omitempty"`
	Nick   string `json:"nick,omitempty"`
	Text   string `json:"text,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// فارسی: ServerFrame فرم JSON جواب‌هایی است که server روی WebSocket push می‌کند.
// فارسی: این frame هم برای نتیجه command و هم برای room_event استفاده می‌شود.
type ServerFrame struct {
	Type     string          `json:"type"`
	OK       bool            `json:"ok,omitempty"`
	Error    string          `json:"error,omitempty"`
	ConnID   string          `json:"conn_id,omitempty"`
	RoomID   string          `json:"room_id,omitempty"`
	Result   any             `json:"result,omitempty"`
	Event    *chat.RoomEvent `json:"event,omitempty"`
	Snapshot any             `json:"snapshot,omitempty"`
}

// فارسی: inboundFrame پیام داخلی actor است، نه frame شبکه.
// فارسی: readLoop با این پیام، JSON خوانده‌شده را به mailbox actor می‌فرستد.
type inboundFrame struct {
	frame ClientFrame
}

// فارسی: socketClosed پیام داخلی بسته شدن socket است.
// فارسی: actor با دریافت این پیام roomهایی را که join کرده leave می‌کند.
type socketClosed struct {
	err error
}
