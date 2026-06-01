آره. یکی از رایج‌ترین use caseهای actor model این است: یک موجود زنده‌ی stateful را actor کنی.

مثلاً:

ChatRoom
GameLobby
UserSession
WebSocketConnection
ShoppingCart
OrderStateMachine
DeviceConnection
یعنی هر چیزی که:

state دارد
همزمان چند نفر/چند goroutine می‌خواهند تغییرش بدهند
ترتیب eventها مهم است
نباید race condition بخورد
گاهی باید query شود
خیلی مناسب actor است.

در Elixir/Erlang نمونه خیلی رایجش GenServer برای chat room است. مثلاً در یک نمونه Elixir ChatServer، خود ChatRoom یک gen_server است، state آن لیست clientهاست، و عملیات‌هایی مثل join، leave، send message و get users دارد. (GitHub) داک‌های آموزشی Elixir هم می‌گویند GenServer معمولاً برای processهای long-running، نگه‌داشتن state، گرفتن پیام و انجام کار background استفاده می‌شود. (GitHub) در Ergo هم maintainer می‌گوید طراحی را شبیه «nano-service architecture» ببین؛ actorها سرویس‌های کوچکی هستند که با message passing حرف می‌زنند. (GitHub)

پس اینجا یک ChatRoom Actor با Ergo می‌زنیم.

ایده معماری
AliceActor ─┐
BobActor   ─┼──> ChatRoomActor
SaraActor  ─┘

ChatRoomActor:
- لیست کاربران را نگه می‌دارد
- join را validate می‌کند
- پیام‌ها را broadcast می‌کند
- لیست کاربران را برمی‌گرداند
  نکته best practice:

ChatRoom state فقط داخل ChatRoomActor تغییر می‌کند.
هیچ map/list مشترکی بین goroutineها نداریم.
پس mutex لازم نیست.
main.go
package main

import (
"fmt"
"log"
"sort"
"strings"
"time"

	ergo "ergo.services/ergo"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

//
// Message Types
//

// Join یعنی یک client می‌خواهد وارد room شود.
//
// این پیام را با Call می‌فرستیم، چون caller جواب می‌خواهد:
// آیا join موفق بود؟
// اگر نه، خطا چه بود؟
type Join struct {
Nick   string
Client gen.PID
}

// JoinResult جواب Join است.
//
// این را به عنوان result برمی‌گردانیم، نه error دوم HandleCall.
// چون error دوم در Ergo بیشتر برای خطای actor/lifecycle است، نه خطای business.
type JoinResult struct {
OK    bool
Error string
}

// Leave یعنی یک کاربر از room خارج شود.
//
// این را با Send می‌فرستیم، چون جواب خاصی لازم ندارد.
type Leave struct {
Nick string
}

// PostMessage یعنی یک کاربر داخل room پیام داده.
//
// این را هم با Send می‌فرستیم.
// چون user لازم نیست منتظر جواب بماند.
type PostMessage struct {
From string
Text string
}

// ListUsers یعنی caller لیست کاربران room را می‌خواهد.
//
// این را با Call می‌فرستیم، چون response می‌خواهیم.
type ListUsers struct{}

// RoomEvent پیامی است که ChatRoomActor به ClientActorها می‌فرستد.
//
// مثلاً:
// - کاربر وارد شد
// - کاربر خارج شد
// - پیام جدید آمد
type RoomEvent struct {
Type  string
From  string
Text  string
Users []string
}

//
// ChatRoom Actor
//

// ChatRoomActor مالک state اتاق چت است.
//
// members map داخلی room است.
// key = nick
// value = PID مربوط به actor آن کاربر
//
// این map فقط داخل همین actor تغییر می‌کند.
// پس نیازی به mutex نداریم.
type ChatRoomActor struct {
act.Actor

	members map[string]gen.PID
}

// newChatRoom factory است.
//
// Ergo برای ساخت actor از factory استفاده می‌کند.
// هر بار Spawn انجام شود، یک instance جدید از ChatRoomActor ساخته می‌شود.
func newChatRoom() gen.ProcessBehavior {
return &ChatRoomActor{}
}

// Init هنگام start شدن actor اجرا می‌شود.
//
// اینجا state اولیه را می‌سازیم.
// در production اینجا می‌توانی config بخوانی، child actor بسازی، monitor تنظیم کنی و غیره.
func (r *ChatRoomActor) Init(args ...any) error {
r.members = make(map[string]gen.PID)

	r.Log().Info("chat room started. pid=%s", r.PID())

	return nil
}

// HandleCall برای requestهایی است که جواب می‌خواهند.
//
// در این مثال:
// - Join باید جواب بدهد که موفق بود یا نه.
// - ListUsers باید لیست کاربران را برگرداند.
func (r *ChatRoomActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
switch req := request.(type) {

	case Join:
		return r.handleJoin(req), nil

	case ListUsers:
		return r.userList(), nil

	default:
		// این خطای business/application است.
		// پس actor را terminate نمی‌کنیم.
		return fmt.Errorf("unknown call request: %T", request), nil
	}
}

// HandleMessage برای پیام‌های async است.
//
// در این مثال:
// - Leave نیازی به جواب ندارد.
// - PostMessage فقط باید broadcast شود.
func (r *ChatRoomActor) HandleMessage(from gen.PID, message any) error {
switch msg := message.(type) {

	case Leave:
		r.handleLeave(msg)
		return nil

	case PostMessage:
		r.handlePostMessage(msg)
		return nil

	default:
		r.Log().Warning("unknown async message: %#v", message)
		return nil
	}
}

// handleJoin منطق join کردن کاربر است.
//
// این تابع private است و فقط داخل actor صدا زده می‌شود.
// یعنی هنوز state فقط توسط خود actor تغییر می‌کند.
func (r *ChatRoomActor) handleJoin(req Join) JoinResult {
nick := strings.TrimSpace(req.Nick)

	if nick == "" {
		return JoinResult{OK: false, Error: "nick is empty"}
	}

	if _, exists := r.members[nick]; exists {
		return JoinResult{OK: false, Error: "nick already exists"}
	}

	r.members[nick] = req.Client

	r.broadcast(RoomEvent{
		Type:  "join",
		From:  nick,
		Text:  nick + " joined the room",
		Users: r.userList(),
	})

	return JoinResult{OK: true}
}

// handleLeave کاربر را از room حذف می‌کند.
//
// چون این پیام async است، caller منتظر جواب نمی‌ماند.
func (r *ChatRoomActor) handleLeave(req Leave) {
if _, exists := r.members[req.Nick]; !exists {
return
}

	delete(r.members, req.Nick)

	r.broadcast(RoomEvent{
		Type:  "leave",
		From:  req.Nick,
		Text:  req.Nick + " left the room",
		Users: r.userList(),
	})
}

// handlePostMessage پیام کاربر را به همه اعضای room broadcast می‌کند.
//
// نکته:
// خود ChatRoomActor message را پردازش می‌کند، اما چاپ/ارسال به clientها
// با پیام جدا به ClientActorها انجام می‌شود.
func (r *ChatRoomActor) handlePostMessage(msg PostMessage) {
if _, exists := r.members[msg.From]; !exists {
r.Log().Warning("message from unknown user: %s", msg.From)
return
}

	r.broadcast(RoomEvent{
		Type: "message",
		From: msg.From,
		Text: msg.Text,
	})
}

// userList لیست کاربران را deterministic برمی‌گرداند.
//
// sort فقط برای این است که خروجی demo مرتب و قابل پیش‌بینی باشد.
func (r *ChatRoomActor) userList() []string {
users := make([]string, 0, len(r.members))

	for nick := range r.members {
		users = append(users, nick)
	}

	sort.Strings(users)

	return users
}

// broadcast به همه client actorها پیام می‌فرستد.
//
// اینجا ChatRoomActor خودش state را تغییر نمی‌دهد؛ فقط event می‌فرستد.
// Send async است، پس room منتظر جواب clientها نمی‌ماند.
func (r *ChatRoomActor) broadcast(event RoomEvent) {
for _, clientPID := range r.members {
_ = r.Send(clientPID, event)
}
}

// Terminate آخر عمر actor است.
//
// اینجا جای cleanup است.
// مثلاً flush کردن buffer، بستن connection، log نهایی.
func (r *ChatRoomActor) Terminate(reason error) {
r.Log().Info("chat room terminated. reason=%v", reason)
}

//
// Client Actor
//

// ClientActor در این demo فقط eventهای room را چاپ می‌کند.
//
// در real world، این actor می‌تواند نماینده WebSocket connection باشد.
// یعنی هر user connection یک actor خودش را دارد.
type ClientActor struct {
act.Actor

	nick string
}

// newClient factory برای ساخت ClientActor است.
func newClient() gen.ProcessBehavior {
return &ClientActor{}
}

// Init نام کاربر را از args می‌گیرد.
//
// موقع Spawn، ما nick را به عنوان arg پاس می‌دهیم.
func (c *ClientActor) Init(args ...any) error {
if len(args) > 0 {
c.nick = args[0].(string)
}

	c.Log().Info("client started. nick=%s pid=%s", c.nick, c.PID())

	return nil
}

// HandleMessage eventهایی را که از ChatRoomActor می‌آیند دریافت می‌کند.
//
// این actor در demo فقط چاپ می‌کند.
// در پروژه واقعی اینجا می‌توانی روی WebSocket بنویسی.
func (c *ClientActor) HandleMessage(from gen.PID, message any) error {
switch event := message.(type) {

	case RoomEvent:
		fmt.Printf("[%s received] type=%s from=%s text=%q users=%v\n",
			c.nick,
			event.Type,
			event.From,
			event.Text,
			event.Users,
		)

	default:
		c.Log().Warning("unknown client message: %#v", message)
	}

	return nil
}

//
// main
//

func main() {
// Node یعنی runtime اصلی Ergo.
// actorها داخل node اجرا می‌شوند.
node, err := ergo.StartNode("chat-demo@localhost", gen.NodeOptions{})
if err != nil {
log.Fatal(err)
}
defer node.Stop()

	// ساختن ChatRoomActor
	roomPID, err := node.Spawn(newChatRoom, gen.ProcessOptions{})
	if err != nil {
		log.Fatal(err)
	}

	// ساختن سه ClientActor
	alicePID, err := node.Spawn(newClient, gen.ProcessOptions{}, "alice")
	if err != nil {
		log.Fatal(err)
	}

	bobPID, err := node.Spawn(newClient, gen.ProcessOptions{}, "bob")
	if err != nil {
		log.Fatal(err)
	}

	saraPID, err := node.Spawn(newClient, gen.ProcessOptions{}, "sara")
	if err != nil {
		log.Fatal(err)
	}

	// Join را با Call می‌زنیم، چون جواب validation می‌خواهیم.
	mustJoin(node, roomPID, Join{Nick: "alice", Client: alicePID})
	mustJoin(node, roomPID, Join{Nick: "bob", Client: bobPID})
	mustJoin(node, roomPID, Join{Nick: "sara", Client: saraPID})

	// گرفتن لیست کاربران با Call
	users, err := node.Call(roomPID, ListUsers{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("current users:", users)

	// پیام دادن async
	_ = node.Send(roomPID, PostMessage{
		From: "alice",
		Text: "سلام بچه‌ها",
	})

	_ = node.Send(roomPID, PostMessage{
		From: "bob",
		Text: "سلام Alice!",
	})

	// خروج async
	_ = node.Send(roomPID, Leave{Nick: "sara"})

	time.Sleep(500 * time.Millisecond)
}

// mustJoin helper ساده برای demo است.
//
// در پروژه واقعی بهتر است error handling جدی‌تر باشد.
func mustJoin(node gen.Node, roomPID gen.PID, req Join) {
result, err := node.Call(roomPID, req)
if err != nil {
log.Fatal(err)
}

	joinResult := result.(JoinResult)
	if !joinResult.OK {
		log.Fatalf("join failed for %s: %s", req.Nick, joinResult.Error)
	}

	fmt.Println(req.Nick, "joined successfully")
}
اجرا
go mod init actor-chat-demo
go get ergo.services/ergo@latest
go run .
اگر هنوز همان خطای Go version را داری، اول باید نصب Go را یک‌دست کنی؛ چون Ergo خودش Go 1.20+ می‌خواهد. (GitHub)

چرا این use case رایج و درست است؟
چون ChatRoomActor دقیقاً یک state owner است:

members map[string]gen.PID
این state را هیچ جای دیگری تغییر نمی‌دهد. همه فقط پیام می‌فرستند:

Join
Leave
PostMessage
ListUsers
پس مدل ذهنی‌اش شبیه Elixir است:

GenServer.call(room, {:join, user})
GenServer.cast(room, {:message, text})
در Ergo می‌شود:

node.Call(roomPID, Join{...})
node.Send(roomPID, PostMessage{...})
این الگو برای پروژه‌های واقعی هم خیلی رایج است:

ChatRoomActor      → اتاق چت
GameLobbyActor     → لابی بازی
OrderActor         → وضعیت سفارش
UserSessionActor   → session کاربر
DeviceActor        → اتصال دستگاه صنعتی / IoT
ReactionActor      → وضعیت reaction یک message یا یک shard
برای use case خودت، مثلاً reaction system، همین الگو می‌شود:

ReactionShardActor
- مالک deltaها
- مالک processing_delta
- تصمیم‌گیرنده flush
- دریافت‌کننده پیام از request path و worker
  یعنی به‌جای اینکه request path و worker هر دو مستقیم Redis را دستکاری کنند، هر دو پیام می‌دهند به actor:

Request Path ─┐
Worker       ─┼──> ReactionShardActor ──> Redis / Scylla
Cron Flush   ─┘
قاعده طلایی actor model:

هر state حساس باید یک مالک sequential داشته باشد.
همین است که actor model را برای سیستم‌های real-time، chat، game lobby، websocket، IoT، order workflow و queue workerها خیلی محبوب کرده.

