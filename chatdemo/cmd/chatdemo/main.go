package main

import (
	"log"

	"actor-chat-demo/internal/app"
)

// فارسی: main فقط نقطه ورود برنامه است؛ منطق اصلی را داخل app نگه داشته‌ایم
// فارسی: تا فایل اصلی کوچک بماند و خواندن پروژه از یک مسیر واضح شروع شود.
func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
