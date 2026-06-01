package httpapi

import (
	"fmt"
	"net/http"

	"actor-chat-demo/internal/chat"
)

// فارسی: listMessages history پیام‌ها را از مسیر actor system می‌خواند.
// فارسی: حتی queryها هم از Registry رد می‌شوند تا room actor lazy hydrate شود.
func (api *Handler) listMessages(w http.ResponseWriter, r *http.Request, roomID string) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	// فارسی: limit از query string خوانده می‌شود و داخل RoomActor clamp می‌شود.
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		_, _ = fmt.Sscanf(raw, "%d", &limit)
	}

	api.writeCall(w, chat.ListMessages{
		RoomID: roomID,
		Limit:  limit,
	})
}

// فارسی: unloadRoom actor فعال یک room را پایین می‌آورد.
// فارسی: Redis state باقی می‌ماند، پس request بعدی دوباره آن را hydrate می‌کند.
func (api *Handler) unloadRoom(w http.ResponseWriter, r *http.Request, roomID string) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	api.writeCall(w, chat.UnloadRoom{RoomID: roomID})
}

// فارسی: requireMethod جلوی استفاده اشتباه از endpointها را می‌گیرد.
func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}

	writeJSON(w, http.StatusMethodNotAllowed, chat.Fail("method not allowed"))
	return false
}
