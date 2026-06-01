package loadtest

import (
	"context"
	"fmt"
	"strings"
)

// فارسی: isMissingSetupError تشخیص می‌دهد خطای message به آماده نبودن room مربوط است یا نه.
// فارسی: فقط این نوع خطاها را self-heal می‌کنیم؛ خطای Redis یا timeout باید fail واقعی بماند.
func isMissingSetupError(err error) bool {
	httpErr, ok := err.(HTTPError)
	if !ok {
		return false
	}

	body := httpErr.Body
	return strings.Contains(body, "room not found") ||
		strings.Contains(body, "sender is not member of room")
}

// فارسی: ensureRoomSetup یک room را idempotent آماده می‌کند.
// فارسی: create room و join userها اگر قبلاً انجام شده باشند، نباید مشکل بسازند.
func ensureRoomSetup(ctx context.Context, cfg Config, client *Client, roomNumber int) error {
	if err := createRoom(ctx, client, roomNumber); err != nil {
		return fmt.Errorf("auto setup create room failed: %w", err)
	}

	for userIndex := 0; userIndex < cfg.UsersPerRoom; userIndex++ {
		if err := joinRoom(ctx, client, roomNumber, userIndex); err != nil {
			return fmt.Errorf("auto setup join room failed: %w", err)
		}
	}

	return nil
}

// فارسی: createRoom همان request ساخت room را مستقیم اجرا می‌کند.
func createRoom(ctx context.Context, client *Client, roomNumber int) error {
	body := map[string]string{
		"room_id": roomID(roomNumber),
		"title":   fmt.Sprintf("Room %d", roomNumber),
	}
	return client.postJSON(ctx, "/rooms", body)
}

// فارسی: joinRoom یک user مشخص را داخل room آماده می‌کند.
func joinRoom(ctx context.Context, client *Client, roomNumber, userIndex int) error {
	body := map[string]string{
		"nick": nickForUser(roomNumber, userIndex),
	}
	return client.postJSON(ctx, "/rooms/"+roomID(roomNumber)+"/join", body)
}
