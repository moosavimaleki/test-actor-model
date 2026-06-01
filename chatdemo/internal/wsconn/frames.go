package wsconn

import "actor-chat-demo/internal/chat"

type ClientFrame struct {
	Type   string `json:"type"`
	RoomID string `json:"room_id,omitempty"`
	Nick   string `json:"nick,omitempty"`
	Text   string `json:"text,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

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

type inboundFrame struct {
	frame ClientFrame
}

type socketClosed struct {
	err error
}
