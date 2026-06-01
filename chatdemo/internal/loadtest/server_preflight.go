package loadtest

import (
	"context"
	"fmt"
)

// فارسی: preflightServer قبل از load واقعی مطمئن می‌شود chat server بالا است.
// فارسی: اگر این check نباشد، با connection refused میلیون‌ها fail بی‌معنی تولید می‌شود.
func preflightServer(cfg Config, client *Client) error {
	if !cfg.ServerCheck {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	var health map[string]any
	if err := client.getJSON(ctx, "/healthz", &health); err != nil {
		return fmt.Errorf(
			"chat server is not reachable at %s: %w. اول Redis و خود server را بالا بیاور: docker run --rm --name ergo-chat-redis -p 6379:6379 redis:7-alpine سپس go run ./cmd/chatdemo",
			cfg.BaseURL,
			err,
		)
	}

	return nil
}
