package chat

import (
	"fmt"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/redis/go-redis/v9"
)

// فارسی: RoomConfig وابستگی‌های لازم برای ساخت یک RoomActor است.
// فارسی: Registry این config را هنگام Spawn به actor می‌دهد.
type RoomConfig struct {
	RoomID string
	Redis  *redis.Client
}

// فارسی: roomMember عضویت یک nick در room را نگه می‌دارد.
// فارسی: pid فقط برای connection زنده است و داخل Redis ذخیره نمی‌شود.
type roomMember struct {
	nick string
	pid  gen.PID
}

// فارسی: RoomActor مالک state یک room مشخص است.
// فارسی: هر room actor جدا دارد، پس roomها روی state هم قفل نمی‌گذارند.
// فارسی: هر actor فقط پیام‌های room خودش را sequential پردازش می‌کند.
type RoomActor struct {
	act.Actor

	// فارسی: roomID identity پایدار دامنه است و بعد از restart هم معنی دارد.
	roomID string
	// فارسی: title از snapshot Redis hydrate می‌شود.
	title string
	// فارسی: redis برای snapshot و history پیام‌ها استفاده می‌شود.
	redis *redis.Client
	// فارسی: members state داغ room است و فقط داخل همین actor تغییر می‌کند.
	members map[string]roomMember
}

// فارسی: NewRoom factory مورد نیاز Ergo برای ساخت RoomActor است.
func NewRoom() gen.ProcessBehavior {
	return &RoomActor{}
}

// فارسی: Init یعنی actor تازه ساخته شده و باید از Redis hydrate شود.
// فارسی: PID قبلی actor persistent نیست؛ identity پایدار ما roomID است.
func (r *RoomActor) Init(args ...any) error {
	if len(args) != 1 {
		return fmt.Errorf("RoomActor needs RoomConfig")
	}

	cfg, ok := args[0].(RoomConfig)
	if !ok {
		return fmt.Errorf("invalid RoomConfig: %T", args[0])
	}

	r.roomID = cfg.RoomID
	r.redis = cfg.Redis
	r.members = make(map[string]roomMember)
	if err := r.hydrate(); err != nil {
		return err
	}

	// فارسی: بعد از hydrate، actor آماده پردازش پیام‌های room است.
	if verboseActorLogs() {
		r.Log().Info("room actor started. room_id=%s pid=%s members=%d", r.roomID, r.PID(), len(r.members))
	}
	return nil
}

// فارسی: HandleCall همه commandهای sync مربوط به یک room را هندل می‌کند.
// فارسی: چون Ergo پیام‌ها را یکی‌یکی اجرا می‌کند، اینجا نیاز به mutex نداریم.
func (r *RoomActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	switch req := request.(type) {
	case JoinRoom:
		return r.join(req.Nick, req.Conn), nil
	case LeaveRoom:
		return r.leave(req.Nick, req.Conn), nil
	case PostMessage:
		return r.postMessage(req.From, req.Text, req.Conn), nil
	case GetRoom:
		return r.snapshot(), nil
	case ListMessages:
		return r.listMessages(req.Limit), nil
	case DrainRegistry:
		// فارسی: DrainRegistry قبل از shutdown صدا زده می‌شود تا snapshot نهایی durable شود.
		if err := r.persistSnapshot(); err != nil {
			return Fail(err.Error()), nil
		}
		return OK(), nil
	default:
		return Fail(fmt.Sprintf("unknown room request: %T", request)), nil
	}
}

// فارسی: Terminate جای کار سنگین نیست.
// فارسی: snapshot نهایی باید قبل از این، در DrainRegistry نوشته شده باشد.
func (r *RoomActor) Terminate(reason error) {
	if verboseActorLogs() {
		r.Log().Info("room actor stopped. room_id=%s reason=%v", r.roomID, reason)
	}
}
