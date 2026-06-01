package httpapi

import (
	"fmt"
	"net/http"

	"actor-chat-demo/internal/chat"
)

// فارسی: jsonMiddleware برای همه responseها Content-Type JSON می‌گذارد.
// فارسی: این middleware قبل از رسیدن request به handler اصلی اجرا می‌شود.
func jsonMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		next.ServeHTTP(w, r)
	})
}

// فارسی: recoverMiddleware جلوی crash شدن کل HTTP server با panic یک handler را می‌گیرد.
// فارسی: panic نباید جایگزین error handling عادی شود؛ فقط safety net است.
func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				writeJSON(w, http.StatusInternalServerError, chat.Fail(fmt.Sprintf("panic: %v", rec)))
			}
		}()

		next.ServeHTTP(w, r)
	})
}
