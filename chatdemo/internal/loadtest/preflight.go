package loadtest

import (
	"context"
	"fmt"
)

type roomSnapshot struct {
	Members []string `json:"members"`
}

// فارسی: preflightMessages قبل از شروع فشار اصلی یک room و یک member را چک می‌کند.
// فارسی: اگر setup اجرا نشده باشد، اینجا سریع و واضح fail می‌کنیم، نه اینکه هزاران request شکست بخورند.
func preflightMessages(cfg Config, client *Client) error {
	if !cfg.Preflight {
		return nil
	}

	roomNumber := cfg.RoomStart
	nick := nickForUser(roomNumber, 0)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	var snapshot roomSnapshot
	if err := client.getJSON(ctx, "/rooms/"+roomID(roomNumber), &snapshot); err != nil {
		return fmt.Errorf("message preflight failed for %s: %w. اول phase setup یا phase all را برای همین range اجرا کن", roomID(roomNumber), err)
	}

	for _, member := range snapshot.Members {
		if member == nick {
			return nil
		}
	}

	return fmt.Errorf("message preflight failed: %s داخل %s عضو نیست. setup را با users-per-room کافی اجرا کن", nick, roomID(roomNumber))
}
