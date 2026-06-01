package chat

// فارسی: Result جواب استاندارد commandهای actorهاست.
// فارسی: در Ergo خروجی دوم HandleCall برای lifecycle actor است.
// فارسی: پس خطاهای business را داخل همین struct برمی‌گردانیم.
type Result struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// فارسی: OK یک helper کوچک برای جواب موفق است.
// فارسی: این helper باعث می‌شود همه actorها شکل جواب موفق یکسانی داشته باشند.
func OK() Result {
	return Result{OK: true}
}

// فارسی: Fail یک helper کوچک برای خطای business است.
// فارسی: این خطا actor را crash نمی‌کند؛ فقط به caller جواب منفی می‌دهد.
func Fail(message string) Result {
	return Result{OK: false, Error: message}
}
