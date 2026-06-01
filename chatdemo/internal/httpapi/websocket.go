package httpapi

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/gorilla/websocket"

	"ergo.services/ergo/gen"

	"actor-chat-demo/internal/wsconn"
)

// فارسی: connSeq برای ساختن id ساده و قابل خواندن برای connectionهاست.
// فارسی: این id فقط برای debug است و identity امنیتی محسوب نمی‌شود.
var connSeq atomic.Uint64

// فارسی: upgrader درخواست HTTP را به WebSocket تبدیل می‌کند.
var upgrader = websocket.Upgrader{
	// فارسی: برای تمرین local آزاد گذاشته شده است.
	// فارسی: در production باید origin را دقیق validate کنی.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// فارسی: websocket برای هر connection یک WSConnectionActor جدا spawn می‌کند.
// فارسی: این یعنی socket state از room state جدا می‌ماند.
func (api *Handler) websocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	connID := fmt.Sprintf("conn-%d", connSeq.Add(1))
	// فارسی: از اینجا به بعد connection متعلق به WSConnectionActor است.
	// فارسی: اگر Spawn fail شود، خودمان connection را می‌بندیم.
	_, err = api.node.Spawn(
		wsconn.New,
		gen.ProcessOptions{},
		wsconn.Config{
			Node:        api.node,
			RegistryPID: api.registryPID,
			ConnID:      connID,
			Conn:        conn,
		},
	)
	if err != nil {
		_ = conn.Close()
	}
}
