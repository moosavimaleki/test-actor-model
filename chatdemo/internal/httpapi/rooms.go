package httpapi

import (
	"net/http"
	"strings"

	"actor-chat-demo/internal/chat"
)

// فارسی: healthz endpoint ساده سلامت process است.
// فارسی: این endpoint به actorها call نمی‌زند تا خودش سبک و قابل اتکا بماند.
func (api *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// فارسی: createRoom درخواست ساخت room را از HTTP به Registry actor تبدیل می‌کند.
func (api *Handler) createRoom(w http.ResponseWriter, r *http.Request) {
	var req chat.CreateRoom
	if !decodeJSON(w, r, &req) {
		return
	}

	api.writeCall(w, req)
}

// فارسی: listRooms لیست roomهای durable را از Registry می‌گیرد.
func (api *Handler) listRooms(w http.ResponseWriter, r *http.Request) {
	api.writeCall(w, chat.ListRooms{})
}

// فارسی: roomSubroutes مسیرهای /rooms/{roomID}/... را route می‌کند.
// فارسی: اینجا فقط path parse می‌شود؛ تصمیم‌های دامنه داخل actorهاست.
func (api *Handler) roomSubroutes(w http.ResponseWriter, r *http.Request) {
	roomID, action, ok := parseRoomPath(r.URL.Path)
	if !ok {
		writeJSON(w, http.StatusNotFound, chat.Fail("not found"))
		return
	}

	if action == "" && r.Method == http.MethodGet {
		api.writeCall(w, chat.GetRoom{RoomID: roomID})
		return
	}

	api.dispatchRoomAction(w, r, roomID, action)
}

// فارسی: parseRoomPath path ساده این پروژه را به roomID و action می‌شکند.
// فارسی: برای پروژه واقعی می‌توانی router جدی‌تر اضافه کنی، ولی اینجا خوانایی آموزشی مهم‌تر است.
func parseRoomPath(path string) (string, string, bool) {
	parts := strings.Split(strings.TrimPrefix(path, "/rooms/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return "", "", false
	}
	if len(parts) == 1 {
		return parts[0], "", true
	}
	if len(parts) == 2 {
		return parts[0], parts[1], true
	}

	return "", "", false
}
