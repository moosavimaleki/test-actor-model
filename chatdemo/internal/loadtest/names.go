package loadtest

import "fmt"

// فارسی: roomID نام room را deterministic می‌سازد.
// فارسی: deterministic بودن باعث می‌شود چند load generator بدون هماهنگی پیچیده shard شوند.
func roomID(roomNumber int) string {
	return fmt.Sprintf("room-%d", roomNumber)
}

// فارسی: nickForUser نام member داخل یک room را می‌سازد.
// فارسی: نام‌ها به room وابسته‌اند تا userهای roomهای مختلف با هم قاطی نشوند.
func nickForUser(roomNumber, userIndex int) string {
	return fmt.Sprintf("user-%d-%d", roomNumber, userIndex)
}
