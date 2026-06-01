package httpapi

import (
	"net/http"
	"runtime"
	"time"

	"actor-chat-demo/internal/chat"
)

// فارسی: debugRuntime وضعیت runtime خود process را برای load test نشان می‌دهد.
// فارسی: این endpoint برای تمرین و benchmark است؛ در production باید پشت auth یا خاموش باشد.
func (api *Handler) debugRuntime(w http.ResponseWriter, r *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	writeJSON(w, http.StatusOK, map[string]any{
		"time":           time.Now().UTC(),
		"goroutines":     runtime.NumGoroutine(),
		"gomaxprocs":     runtime.GOMAXPROCS(0),
		"alloc_bytes":    mem.Alloc,
		"heap_alloc":     mem.HeapAlloc,
		"heap_sys":       mem.HeapSys,
		"heap_objects":   mem.HeapObjects,
		"total_alloc":    mem.TotalAlloc,
		"num_gc":         mem.NumGC,
		"next_gc_bytes":  mem.NextGC,
		"last_gc_unixns": mem.LastGC,
	})
}

// فارسی: debugActors تعداد actorهای دامنه را سبک و بدون لیست بزرگ roomها نشان می‌دهد.
// فارسی: برای تست ۱ میلیون room از این endpoint استفاده کن، نه از GET /rooms.
func (api *Handler) debugActors(w http.ResponseWriter, r *http.Request) {
	api.writeCall(w, chat.CountActiveRooms{})
}
