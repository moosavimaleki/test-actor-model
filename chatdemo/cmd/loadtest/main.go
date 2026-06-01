package main

import (
	"log"

	"actor-chat-demo/internal/loadtest"
)

// فارسی: این main فقط ورودی loadtest است.
// فارسی: منطق تست را داخل internal/loadtest نگه داشته‌ایم تا فایل command کوتاه بماند.
func main() {
	if err := loadtest.Run(); err != nil {
		log.Fatal(err)
	}
}
