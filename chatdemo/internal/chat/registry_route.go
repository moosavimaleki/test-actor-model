package chat

import (
	"context"
	"time"
)

// فارسی: routeToRoom یک helper مرکزی برای requestهای مربوط به room است.
// فارسی: اول actor room را ensure می‌کند، بعد command را به همان actor Call می‌زند.
func (rg *RoomRegistryActor) routeToRoom(roomID string, request any) (any, error) {
	pid, res := rg.ensureRoom(roomID)
	if !res.OK {
		return res, nil
	}

	return rg.Call(pid, request)
}

// فارسی: listRooms لیست roomهای durable را از Redis می‌خواند.
// فارسی: active_count فقط تعداد actorهای فعال در همین runtime را نشان می‌دهد.
func (rg *RoomRegistryActor) listRooms() any {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rooms, err := rg.redis.SMembers(ctx, roomsKey()).Result()
	if err != nil {
		return Fail("redis list rooms failed: " + err.Error())
	}

	return map[string]any{
		"rooms":        rooms,
		"active_count": len(rg.rooms),
	}
}

// فارسی: unloadRoom actor یک room را پایین می‌آورد ولی state را حذف نمی‌کند.
// فارسی: این برای آزاد کردن حافظه roomهای idle مفید است.
func (rg *RoomRegistryActor) unloadRoom(roomID string) Result {
	pid, exists := rg.rooms[roomID]
	if !exists {
		return OK()
	}

	// فارسی: قبل از کشتن actor، snapshot نهایی را می‌گیریم.
	// فارسی: این تفاوت unload سالم با kill خام است.
	resAny, err := rg.Call(pid, DrainRegistry{})
	if err != nil {
		return Fail("room drain failed: " + err.Error())
	}
	if res, ok := resAny.(Result); ok && !res.OK {
		return res
	}

	_ = rg.SendExit(pid, genTerminateShutdown())
	delete(rg.rooms, roomID)

	rg.Log().Info("room actor unloaded. room_id=%s active_rooms=%d", roomID, len(rg.rooms))
	return OK()
}
