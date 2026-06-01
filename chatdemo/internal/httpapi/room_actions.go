package httpapi

import (
	"net/http"

	"actor-chat-demo/internal/chat"
)

// فارسی: dispatchRoomAction بخش دوم مسیر را به handler مناسب وصل می‌کند.
// فارسی: مثل /join یا /message، اما خود ruleها همچنان در RoomActor هستند.
func (api *Handler) dispatchRoomAction(w http.ResponseWriter, r *http.Request, roomID, action string) {
	switch action {
	case "join":
		api.joinRoom(w, r, roomID)
	case "leave":
		api.leaveRoom(w, r, roomID)
	case "message":
		api.postMessage(w, r, roomID)
	case "messages":
		api.listMessages(w, r, roomID)
	case "unload":
		api.unloadRoom(w, r, roomID)
	default:
		writeJSON(w, http.StatusNotFound, chat.Fail("not found"))
	}
}

// فارسی: joinRoom body HTTP را به JoinRoom command تبدیل می‌کند.
// فارسی: در HTTP معمولی Conn نداریم، پس join فقط در snapshot می‌نشیند و push زنده ندارد.
func (api *Handler) joinRoom(w http.ResponseWriter, r *http.Request, roomID string) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	var req chat.JoinRoom
	if !decodeJSON(w, r, &req) {
		return
	}
	req.RoomID = roomID
	api.writeCall(w, req)
}

// فارسی: leaveRoom خروج کاربر را از طریق Registry به RoomActor می‌رساند.
func (api *Handler) leaveRoom(w http.ResponseWriter, r *http.Request, roomID string) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	var req chat.LeaveRoom
	if !decodeJSON(w, r, &req) {
		return
	}
	req.RoomID = roomID
	api.writeCall(w, req)
}

// فارسی: postMessage مسیر HTTP ارسال پیام است.
// فارسی: چون HTTP اتصال زنده ندارد، From باید داخل body بیاید.
func (api *Handler) postMessage(w http.ResponseWriter, r *http.Request, roomID string) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	var req chat.PostMessage
	if !decodeJSON(w, r, &req) {
		return
	}
	req.RoomID = roomID
	api.writeCall(w, req)
}
