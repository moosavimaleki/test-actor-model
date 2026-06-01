package chat

import (
	"fmt"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/redis/go-redis/v9"
)

// فارسی: RegistryConfig وابستگی‌های Registry actor را موقع Spawn منتقل می‌کند.
// فارسی: فعلاً فقط Redis لازم داریم چون room list و snapshotها آنجا هستند.
type RegistryConfig struct {
	Redis *redis.Client
}

// فارسی: RoomRegistryActor فقط routing و lifecycle را نگه می‌دارد.
// فارسی: این actor state همه roomها را داخل خودش نگه نمی‌دارد.
// فارسی: فقط می‌داند actor فعال هر room کدام PID است.
type RoomRegistryActor struct {
	act.Actor

	// فارسی: redis برای چک کردن roomهای موجود و ساخت snapshot اولیه استفاده می‌شود.
	redis *redis.Client
	// فارسی: rooms فقط actorهای فعال همین runtime را نگه می‌دارد.
	// فارسی: بعد از restart این map خالی است و roomها lazy hydrate می‌شوند.
	rooms map[string]gen.PID
	// فارسی: draining وقتی true می‌شود که برنامه در حال shutdown است.
	// فارسی: در این حالت requestهای جدید را قبول نمی‌کنیم.
	draining bool
}

// فارسی: NewRegistry factory مورد نیاز Ergo برای ساخت actor است.
// فارسی: هر بار Spawn صدا زده شود، یک instance تازه از Registry ساخته می‌شود.
func NewRegistry() gen.ProcessBehavior {
	return &RoomRegistryActor{}
}

// فارسی: Init اولین callback actor است.
// فارسی: اینجا config را می‌گیریم و state داخلی Registry را آماده می‌کنیم.
func (rg *RoomRegistryActor) Init(args ...any) error {
	if len(args) != 1 {
		return fmt.Errorf("RoomRegistryActor needs RegistryConfig")
	}

	cfg, ok := args[0].(RegistryConfig)
	if !ok {
		return fmt.Errorf("invalid RegistryConfig: %T", args[0])
	}

	rg.redis = cfg.Redis
	rg.rooms = make(map[string]gen.PID)
	rg.Log().Info("room registry started. pid=%s", rg.PID())
	return nil
}

// فارسی: HandleCall مسیر همه requestهای sync به Registry است.
// فارسی: HTTP و WebSocket هر دو در نهایت به همین callback می‌رسند.
func (rg *RoomRegistryActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	// فارسی: وقتی draining هستیم، فقط خود پیام drain مجاز است.
	// فارسی: این کار جلوی ورود mutation جدید وسط shutdown را می‌گیرد.
	if rg.draining {
		if _, isDrain := request.(DrainRegistry); !isDrain {
			return Fail("server is draining"), nil
		}
	}

	switch req := request.(type) {
	case CreateRoom:
		// فارسی: ساخت room باید در Registry انجام شود چون Registry lifecycle roomها را می‌شناسد.
		return rg.createRoom(req), nil
	case JoinRoom:
		// فارسی: JoinRoom به room مناسب route می‌شود؛ خود Registry عضوها را نگه نمی‌دارد.
		return rg.routeToRoom(req.RoomID, JoinRoom{Nick: req.Nick, Conn: req.Conn})
	case LeaveRoom:
		return rg.routeToRoom(req.RoomID, LeaveRoom{Nick: req.Nick, Conn: req.Conn})
	case PostMessage:
		return rg.routeToRoom(req.RoomID, PostMessage{From: req.From, Text: req.Text, Conn: req.Conn})
	case GetRoom:
		return rg.routeToRoom(req.RoomID, req)
	case ListMessages:
		return rg.routeToRoom(req.RoomID, req)
	case ListRooms:
		return rg.listRooms(), nil
	case UnloadRoom:
		return rg.unloadRoom(req.RoomID), nil
	case DrainRegistry:
		return rg.drainAllRooms(), nil
	default:
		return Fail(fmt.Sprintf("unknown registry request: %T", request)), nil
	}
}

// فارسی: Terminate هنگام پایان Registry صدا زده می‌شود.
// فارسی: کار سنگین shutdown در drainAllRooms انجام شده، پس اینجا فقط log داریم.
func (rg *RoomRegistryActor) Terminate(reason error) {
	rg.Log().Info("room registry stopped. reason=%v", reason)
}
