package chat

import (
	"strings"

	"ergo.services/ergo/gen"
)

// فارسی: drainAllRooms برای shutdown یا deploy امن استفاده می‌شود.
// فارسی: اول همه roomها snapshot می‌زنند، بعد actorها stop می‌شوند.
func (rg *RoomRegistryActor) drainAllRooms() Result {
	rg.draining = true

	// فارسی: مرحله اول فقط persistence است؛ هنوز actorها را stop نمی‌کنیم.
	// فارسی: اگر یکی fail شود، حداقل می‌دانیم کدام room مشکل داشته است.
	for roomID, pid := range rg.rooms {
		resAny, err := rg.Call(pid, DrainRegistry{})
		if err != nil {
			// فارسی: اگر PID قبلاً از بین رفته باشد، دیگر چیزی برای drain کردن نداریم.
			// فارسی: در تست‌های سنگین بهتر است shutdown ادامه پیدا کند و map تمیز شود.
			if strings.Contains(err.Error(), "unknown process") {
				delete(rg.rooms, roomID)
				continue
			}
			return Fail("drain room " + roomID + " failed: " + err.Error())
		}

		if res, ok := resAny.(Result); ok && !res.OK {
			return Fail("drain room " + roomID + " failed: " + res.Error)
		}
	}

	// فارسی: مرحله دوم خاموش کردن actorهای room است.
	// فارسی: بعد از این، request جدیدی نباید وارد سیستم شود.
	for roomID, pid := range rg.rooms {
		_ = rg.SendExit(pid, genTerminateShutdown())
		delete(rg.rooms, roomID)
	}

	return OK()
}

// فارسی: این helper فقط reason استاندارد shutdown Ergo را یک‌جا نگه می‌دارد.
func genTerminateShutdown() error {
	return gen.TerminateReasonShutdown
}
