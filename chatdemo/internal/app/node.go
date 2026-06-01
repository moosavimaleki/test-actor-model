package app

import (
	ergo "ergo.services/ergo"
	"ergo.services/ergo/gen"
	"github.com/redis/go-redis/v9"

	"actor-chat-demo/internal/chat"
)

// فارسی: startNode runtime اصلی Ergo را می‌سازد و Registry actor را بالا می‌آورد.
// فارسی: خروجی دوم PID رجیستری است؛ از این به بعد همه requestها به همین PID route می‌شوند.
func startNode(rdb *redis.Client) (gen.Node, gen.PID, error) {
	node, err := ergo.StartNode("chat-v3@localhost", gen.NodeOptions{})
	if err != nil {
		return nil, gen.PID{}, err
	}

	// فارسی: Registry فقط lifecycle و routing را نگه می‌دارد.
	// فارسی: state هر room داخل RoomActor همان room می‌ماند.
	registryPID, err := node.Spawn(
		chat.NewRegistry,
		gen.ProcessOptions{},
		chat.RegistryConfig{Redis: rdb},
	)
	if err != nil {
		node.Stop()
		return nil, gen.PID{}, err
	}

	return node, registryPID, nil
}
