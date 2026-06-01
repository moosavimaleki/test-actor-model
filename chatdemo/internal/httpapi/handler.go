package httpapi

import (
	"net/http"

	"ergo.services/ergo/gen"
)

// فارسی: Handler پل بین HTTP/WebSocket و actor system است.
// فارسی: state اصلی همچنان داخل actorهاست؛ این لایه فقط protocol adapter است.
type Handler struct {
	// فارسی: node برای Spawn کردن WSConnectionActor و Call زدن به actorها لازم است.
	node gen.Node
	// فارسی: registryPID آدرس runtime رجیستری است و همه requestهای دامنه از آن عبور می‌کنند.
	registryPID gen.PID
}

// فارسی: New تمام routeهای HTTP و WebSocket را می‌سازد.
// فارسی: دقت کن routeها فقط adapter هستند و business rule داخل actorهاست.
func New(node gen.Node, registryPID gen.PID) http.Handler {
	api := &Handler{
		node:        node,
		registryPID: registryPID,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", api.healthz)
	mux.HandleFunc("GET /debug/runtime", api.debugRuntime)
	mux.HandleFunc("GET /debug/actors", api.debugActors)
	mux.HandleFunc("GET /ws", api.websocket)
	mux.HandleFunc("GET /rooms", api.listRooms)
	mux.HandleFunc("POST /rooms", api.createRoom)
	// فارسی: مسیرهای زیر /rooms/{roomID}/... دستی parse می‌شوند تا dependency اضافی router نداشته باشیم.
	mux.HandleFunc("/rooms/", api.roomSubroutes)

	return recoverMiddleware(jsonMiddleware(mux))
}
