
آره. من اول جمع‌بندی تحقیق را بگویم، بعد کد.

من نمونه آماده و تمیزِ دقیقاً `Ergo + Redis + HTTP API + graceful shutdown` پیدا نکردم؛ ولی چند نکته معتبر از داک و بحث‌های Ergo مهم است:

Ergo می‌گوید هر process/actor پیام‌ها را sequential در goroutine خودش پردازش می‌کند و mailbox دارد؛ پس actor مالک state می‌شود و race روی state داخلی‌اش حذف می‌شود. ([Ergo Framework Documentation][1])

برای HTTP، داک Ergo خودش می‌گوید برای اکثر APIهای عادی از **standard HTTP handlers + `node.Call()`** شروع کن؛ `WebWorker` و meta-process بیشتر وقتی لازم می‌شود که بخواهی requestها خودشان actor-message شوند یا WebSocket/long-lived connection داشته باشی. ([Ergo Framework Documentation][2])

maintainer Ergo هم در بحث real-world گفته برای DB لازم نیست meta-process بسازی؛ actorها را مثل nano-serviceهایی ببین که با message passing حرف می‌زنند. ([GitHub][3])

پس مثال ما این می‌شود:

```text
HTTP API
   ↓ Call / Send
ChatRoomActor  ← مالک state اتاق
   ↓
Redis persistence
```

## سناریو

یک chat room داریم:

```text
POST /join       ورود کاربر
POST /leave      خروج کاربر
POST /message    ارسال پیام
GET  /users      لیست کاربران
GET  /messages   لیست پیام‌های اخیر
GET  /healthz    سلامت سرویس
```

Redis:

```text
chat:general:users      SET
chat:general:messages   LIST
```

برای graceful shutdown هم از `http.Server.Shutdown` استفاده می‌کنیم. خود داک Go می‌گوید `Shutdown` بدون قطع active connectionها سرور را خاموش می‌کند، listenerها را می‌بندد، idle connectionها را می‌بندد و منتظر idle شدن connectionهای فعال می‌ماند. ([Go Packages][4]) همچنین برای گرفتن `SIGINT/SIGTERM` از `signal.NotifyContext` استفاده می‌کنیم که context را هنگام signal cancel می‌کند. ([Go Packages][5])

---

## نصب Redis برای تست

```bash
docker run --rm --name ergo-chat-redis -p 6379:6379 redis:7-alpine
```

## ساخت پروژه

```bash
mkdir ergo-chat-api
cd ergo-chat-api

go mod init ergo-chat-api
go get ergo.services/ergo@latest
go get github.com/redis/go-redis/v9
```

## `main.go`

```go
package main

import (
"context"
"encoding/json"
"errors"
"fmt"
"log"
"net/http"
"os"
"os/signal"
"sort"
"strings"
"syscall"
"time"

"github.com/redis/go-redis/v9"

ergo "ergo.services/ergo"
"ergo.services/ergo/act"
"ergo.services/ergo/gen"
)

//
// =========================
// Message / DTO Types
// =========================
//

// Join پیام sync برای ورود کاربر به room است.
//
// چرا sync؟
// چون API باید بداند join موفق بوده یا نه.
type Join struct {
Nick string `json:"nick"`
}

// Leave پیام sync برای خروج کاربر است.
//
// این را هم sync گرفتیم تا اگر Redis خطا داد، API بتواند error بدهد.
type Leave struct {
Nick string `json:"nick"`
}

// PostMessage پیام sync برای ارسال پیام است.
//
// چون پیام باید در Redis ذخیره شود، بهتر است API بعد از موفقیت Redis جواب 201 بدهد.
type PostMessage struct {
From string `json:"from"`
Text string `json:"text"`
}

// ListUsers درخواست sync برای گرفتن users فعلی است.
type ListUsers struct{}

// ListMessages درخواست sync برای گرفتن پیام‌های اخیر از Redis است.
type ListMessages struct {
Limit int `json:"limit"`
}

// ChatMessage چیزی است که داخل Redis ذخیره می‌کنیم.
type ChatMessage struct {
From      string    `json:"from"`
Text      string    `json:"text"`
CreatedAt time.Time `json:"created_at"`
}

// OKResult جواب عمومی برای operationهای موفق/ناموفق است.
//
// این را به جای اینکه error دوم HandleCall را پر کنیم برمی‌گردانیم.
// چون error دوم در Ergo بیشتر برای lifecycle actor است، نه business error.
type OKResult struct {
OK    bool   `json:"ok"`
Error string `json:"error,omitempty"`
}

//
// =========================
// ChatRoom Actor
// =========================
//

// ChatRoomActor مالک state اتاق چت است.
//
// نکته مهم:
// users فقط داخل همین actor تغییر می‌کند.
// چون actor پیام‌ها را یکی‌یکی پردازش می‌کند، این map mutex نمی‌خواهد.
type ChatRoomActor struct {
act.Actor

roomName    string
redis       *redis.Client
users       map[string]struct{}
maxMessages int
}

// ChatRoomConfig کانفیگی است که موقع Spawn به actor می‌دهیم.
type ChatRoomConfig struct {
RoomName    string
Redis       *redis.Client
MaxMessages int
}

// newChatRoomActor factory ساخت actor است.
//
// Ergo هنگام Spawn این factory را صدا می‌زند تا یک instance جدید بسازد.
func newChatRoomActor() gen.ProcessBehavior {
return &ChatRoomActor{}
}

// Init شروع زندگی actor است.
//
// اینجا:
// 1. config را از args می‌گیریم
// 2. state داخلی را initialize می‌کنیم
// 3. users قبلی را از Redis restore می‌کنیم
func (a *ChatRoomActor) Init(args ...any) error {
if len(args) != 1 {
return fmt.Errorf("ChatRoomActor needs ChatRoomConfig")
}

cfg, ok := args[0].(ChatRoomConfig)
if !ok {
return fmt.Errorf("invalid config type: %T", args[0])
}

a.roomName = cfg.RoomName
a.redis = cfg.Redis
a.maxMessages = cfg.MaxMessages
a.users = make(map[string]struct{})

if a.maxMessages <= 0 {
a.maxMessages = 100
}

// هنگام start، users را از Redis می‌خوانیم تا اگر app restart شد،
// state اولیه actor از persistence برگردد.
if err := a.loadUsersFromRedis(); err != nil {
return err
}

a.Log().Info("chat room started. room=%s users=%d", a.roomName, len(a.users))

return nil
}

// HandleCall مسیر requestهای sync است.
//
// API از node.Call استفاده می‌کند و اینجا جواب می‌گیرد.
func (a *ChatRoomActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
switch req := request.(type) {

case Join:
return a.handleJoin(req), nil

case Leave:
return a.handleLeave(req), nil

case PostMessage:
return a.handlePostMessage(req), nil

case ListUsers:
return a.handleListUsers(), nil

case ListMessages:
return a.handleListMessages(req), nil

default:
// business error، نه actor crash
return OKResult{
OK:    false,
Error: fmt.Sprintf("unknown request: %T", request),
}, nil
}
}

// Terminate آخر عمر actor است.
//
// چون Redis client در main ساخته شده، اینجا نمی‌بندیمش.
// فقط log می‌زنیم. بستن Redis در main بعد از node.Stop انجام می‌شود.
func (a *ChatRoomActor) Terminate(reason error) {
a.Log().Info("chat room stopped. room=%s reason=%v", a.roomName, reason)
}

// handleJoin کاربر را وارد room می‌کند.
//
// ترتیب مهم است:
// 1. validate
// 2. Redis SADD
// 3. update memory state
//
// اگر Redis fail شود، state داخلی را تغییر نمی‌دهیم تا memory و Redis diverge نشوند.
func (a *ChatRoomActor) handleJoin(req Join) OKResult {
nick := strings.TrimSpace(req.Nick)
if nick == "" {
return OKResult{OK: false, Error: "nick is empty"}
}

if _, exists := a.users[nick]; exists {
return OKResult{OK: true}
}

ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

if err := a.redis.SAdd(ctx, a.usersKey(), nick).Err(); err != nil {
return OKResult{OK: false, Error: "redis SADD failed: " + err.Error()}
}

a.users[nick] = struct{}{}

return OKResult{OK: true}
}

// handleLeave کاربر را از room حذف می‌کند.
//
// باز هم اول Redis، بعد memory.
// این باعث می‌شود اگر persistence fail شد، state داخلی optimistic جلو نرود.
func (a *ChatRoomActor) handleLeave(req Leave) OKResult {
nick := strings.TrimSpace(req.Nick)
if nick == "" {
return OKResult{OK: false, Error: "nick is empty"}
}

ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

if err := a.redis.SRem(ctx, a.usersKey(), nick).Err(); err != nil {
return OKResult{OK: false, Error: "redis SREM failed: " + err.Error()}
}

delete(a.users, nick)

return OKResult{OK: true}
}

// handlePostMessage پیام را validate و persist می‌کند.
//
// اینجا actor مالک rule است:
// اگر sender داخل room نیست، اجازه message ندارد.
func (a *ChatRoomActor) handlePostMessage(req PostMessage) OKResult {
from := strings.TrimSpace(req.From)
text := strings.TrimSpace(req.Text)

if from == "" {
return OKResult{OK: false, Error: "from is empty"}
}
if text == "" {
return OKResult{OK: false, Error: "text is empty"}
}

if _, exists := a.users[from]; !exists {
return OKResult{OK: false, Error: "sender is not in the room"}
}

msg := ChatMessage{
From:      from,
Text:      text,
CreatedAt: time.Now().UTC(),
}

payload, err := json.Marshal(msg)
if err != nil {
return OKResult{OK: false, Error: "marshal failed: " + err.Error()}
}

ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

// Redis LIST:
// RPUSH یعنی پیام جدید آخر لیست می‌رود.
// LTRIM با -maxMessages تا -1 یعنی فقط آخرین N پیام نگه داشته شود.
pipe := a.redis.TxPipeline()
pipe.RPush(ctx, a.messagesKey(), payload)
pipe.LTrim(ctx, a.messagesKey(), int64(-a.maxMessages), -1)

if _, err := pipe.Exec(ctx); err != nil {
return OKResult{OK: false, Error: "redis message write failed: " + err.Error()}
}

return OKResult{OK: true}
}

// handleListUsers لیست users را از memory می‌دهد.
//
// چون ChatRoomActor مالک users است، این read هم safe است.
func (a *ChatRoomActor) handleListUsers() []string {
users := make([]string, 0, len(a.users))
for nick := range a.users {
users = append(users, nick)
}

sort.Strings(users)

return users
}

// handleListMessages پیام‌های اخیر را از Redis می‌خواند.
//
// اینجا از Redis می‌خوانیم، نه memory.
// چون messages را برای persistence در Redis نگه داشته‌ایم.
func (a *ChatRoomActor) handleListMessages(req ListMessages) any {
limit := req.Limit
if limit <= 0 || limit > a.maxMessages {
limit = a.maxMessages
}

ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

// چون با RPUSH ذخیره کرده‌ایم، LRANGE -limit -1 آخرین N پیام را
// با ترتیب قدیمی‌تر به جدیدتر برمی‌گرداند.
rows, err := a.redis.LRange(ctx, a.messagesKey(), int64(-limit), -1).Result()
if err != nil {
return OKResult{OK: false, Error: "redis LRANGE failed: " + err.Error()}
}

messages := make([]ChatMessage, 0, len(rows))
for _, row := range rows {
var msg ChatMessage
if err := json.Unmarshal([]byte(row), &msg); err != nil {
return OKResult{OK: false, Error: "unmarshal message failed: " + err.Error()}
}
messages = append(messages, msg)
}

return messages
}

// loadUsersFromRedis هنگام start actor صدا زده می‌شود.
//
// اگر app restart شد، users از Redis به memory برمی‌گردند.
func (a *ChatRoomActor) loadUsersFromRedis() error {
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

users, err := a.redis.SMembers(ctx, a.usersKey()).Result()
if err != nil {
return fmt.Errorf("redis SMEMBERS failed: %w", err)
}

for _, nick := range users {
nick = strings.TrimSpace(nick)
if nick != "" {
a.users[nick] = struct{}{}
}
}

return nil
}

// usersKey کلید Redis برای کاربران room است.
func (a *ChatRoomActor) usersKey() string {
return "chat:" + a.roomName + ":users"
}

// messagesKey کلید Redis برای پیام‌های room است.
func (a *ChatRoomActor) messagesKey() string {
return "chat:" + a.roomName + ":messages"
}

//
// =========================
// HTTP API
// =========================
//

// APIHandler bridge بین HTTP و actor است.
//
// این struct خودش state اصلی ندارد.
// فقط request را parse می‌کند و به actor Call می‌زند.
type APIHandler struct {
node    gen.Node
roomPID gen.PID
}

// newAPIHandler هندلرها را می‌سازد.
func newAPIHandler(node gen.Node, roomPID gen.PID) http.Handler {
api := &APIHandler{
node:    node,
roomPID: roomPID,
}

mux := http.NewServeMux()

mux.HandleFunc("GET /healthz", api.healthz)
mux.HandleFunc("POST /join", api.join)
mux.HandleFunc("POST /leave", api.leave)
mux.HandleFunc("POST /message", api.postMessage)
mux.HandleFunc("GET /users", api.listUsers)
mux.HandleFunc("GET /messages", api.listMessages)

return recoverMiddleware(jsonMiddleware(mux))
}

// healthz برای health check است.
func (api *APIHandler) healthz(w http.ResponseWriter, r *http.Request) {
writeJSON(w, http.StatusOK, map[string]any{
"ok": true,
})
}

// join: POST /join
//
// body:
// {"nick":"alice"}
func (api *APIHandler) join(w http.ResponseWriter, r *http.Request) {
var req Join
if !decodeJSON(w, r, &req) {
return
}

result, err := api.node.Call(api.roomPID, req)
if err != nil {
writeJSON(w, http.StatusInternalServerError, OKResult{OK: false, Error: err.Error()})
return
}

ok := result.(OKResult)
if !ok.OK {
writeJSON(w, http.StatusBadRequest, ok)
return
}

writeJSON(w, http.StatusOK, ok)
}

// leave: POST /leave
//
// body:
// {"nick":"alice"}
func (api *APIHandler) leave(w http.ResponseWriter, r *http.Request) {
var req Leave
if !decodeJSON(w, r, &req) {
return
}

result, err := api.node.Call(api.roomPID, req)
if err != nil {
writeJSON(w, http.StatusInternalServerError, OKResult{OK: false, Error: err.Error()})
return
}

ok := result.(OKResult)
if !ok.OK {
writeJSON(w, http.StatusBadRequest, ok)
return
}

writeJSON(w, http.StatusOK, ok)
}

// postMessage: POST /message
//
// body:
// {"from":"alice","text":"سلام"}
func (api *APIHandler) postMessage(w http.ResponseWriter, r *http.Request) {
var req PostMessage
if !decodeJSON(w, r, &req) {
return
}

result, err := api.node.Call(api.roomPID, req)
if err != nil {
writeJSON(w, http.StatusInternalServerError, OKResult{OK: false, Error: err.Error()})
return
}

ok := result.(OKResult)
if !ok.OK {
writeJSON(w, http.StatusBadRequest, ok)
return
}

writeJSON(w, http.StatusCreated, ok)
}

// listUsers: GET /users
func (api *APIHandler) listUsers(w http.ResponseWriter, r *http.Request) {
result, err := api.node.Call(api.roomPID, ListUsers{})
if err != nil {
writeJSON(w, http.StatusInternalServerError, OKResult{OK: false, Error: err.Error()})
return
}

writeJSON(w, http.StatusOK, map[string]any{
"users": result,
})
}

// listMessages: GET /messages?limit=20
func (api *APIHandler) listMessages(w http.ResponseWriter, r *http.Request) {
limit := 20

if raw := r.URL.Query().Get("limit"); raw != "" {
_, _ = fmt.Sscanf(raw, "%d", &limit)
}

result, err := api.node.Call(api.roomPID, ListMessages{Limit: limit})
if err != nil {
writeJSON(w, http.StatusInternalServerError, OKResult{OK: false, Error: err.Error()})
return
}

// اگر actor به جای []ChatMessage یک OKResult خطادار برگرداند.
if ok, isOK := result.(OKResult); isOK && !ok.OK {
writeJSON(w, http.StatusInternalServerError, ok)
return
}

writeJSON(w, http.StatusOK, map[string]any{
"messages": result,
})
}

//
// =========================
// HTTP Helpers
// =========================
//

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
defer r.Body.Close()

decoder := json.NewDecoder(r.Body)
decoder.DisallowUnknownFields()

if err := decoder.Decode(dst); err != nil {
writeJSON(w, http.StatusBadRequest, OKResult{
OK:    false,
Error: "invalid json: " + err.Error(),
})
return false
}

return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
w.WriteHeader(status)

if err := json.NewEncoder(w).Encode(body); err != nil {
log.Printf("write json failed: %v", err)
}
}

func jsonMiddleware(next http.Handler) http.Handler {
return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json; charset=utf-8")
next.ServeHTTP(w, r)
})
}

func recoverMiddleware(next http.Handler) http.Handler {
return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
defer func() {
if rec := recover(); rec != nil {
writeJSON(w, http.StatusInternalServerError, OKResult{
OK:    false,
Error: fmt.Sprintf("panic: %v", rec),
})
}
}()

next.ServeHTTP(w, r)
})
}

//
// =========================
// main + Graceful Shutdown
// =========================
//

func main() {
// signalCtx وقتی SIGINT یا SIGTERM برسد cancel می‌شود.
// SIGINT یعنی Ctrl+C.
// SIGTERM یعنی shutdown معمول container/systemd.
signalCtx, stopSignals := signal.NotifyContext(
context.Background(),
os.Interrupt,
syscall.SIGTERM,
)
defer stopSignals()

// Redis client
rdb := redis.NewClient(&redis.Options{
Addr: "localhost:6379",
})

if err := pingRedis(signalCtx, rdb); err != nil {
log.Fatalf("redis is not ready: %v", err)
}

// Ergo node
node, err := ergo.StartNode("chat-api@localhost", gen.NodeOptions{})
if err != nil {
log.Fatal(err)
}

// ChatRoomActor را spawn می‌کنیم.
// این actor مالک state اتاق است.
roomPID, err := node.Spawn(
newChatRoomActor,
gen.ProcessOptions{},
ChatRoomConfig{
RoomName:    "general",
Redis:       rdb,
MaxMessages: 100,
},
)
if err != nil {
log.Fatal(err)
}

// HTTP server
server := &http.Server{
Addr:              ":8080",
Handler:           newAPIHandler(node, roomPID),
ReadHeaderTimeout: 5 * time.Second,
ReadTimeout:       10 * time.Second,
WriteTimeout:      10 * time.Second,
IdleTimeout:       60 * time.Second,
}

// سرور را در goroutine جدا بالا می‌آوریم تا main بتواند منتظر signal بماند.
serverErr := make(chan error, 1)
go func() {
log.Println("HTTP API listening on :8080")

err := server.ListenAndServe()
if err != nil && !errors.Is(err, http.ErrServerClosed) {
serverErr <- err
return
}

serverErr <- nil
}()

// منتظر می‌مانیم یا signal بیاید یا server error بدهد.
select {
case <-signalCtx.Done():
log.Println("shutdown signal received")

case err := <-serverErr:
if err != nil {
log.Fatalf("http server failed: %v", err)
}
}

// نکته مهم:
// برای Shutdown نباید از signalCtx استفاده کنیم، چون آن context already canceled است.
// یک context تازه با timeout می‌سازیم.
shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
defer cancelShutdown()

// اول HTTP را shutdown می‌کنیم تا request جدید وارد actor نشود.
if err := server.Shutdown(shutdownCtx); err != nil {
log.Printf("http graceful shutdown failed: %v", err)
} else {
log.Println("http server stopped")
}

// بعد Ergo node را stop می‌کنیم تا actorها terminate شوند.
node.Stop()
log.Println("ergo node stopped")

// آخر Redis client را می‌بندیم.
if err := rdb.Close(); err != nil {
log.Printf("redis close failed: %v", err)
} else {
log.Println("redis client closed")
}

log.Println("shutdown complete")
}

func pingRedis(ctx context.Context, rdb *redis.Client) error {
pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()

return rdb.Ping(pingCtx).Err()
}
```

## تست با curl

```bash
curl -X POST localhost:8080/join \
  -H 'Content-Type: application/json' \
  -d '{"nick":"alice"}'
```

```bash
curl -X POST localhost:8080/join \
  -H 'Content-Type: application/json' \
  -d '{"nick":"bob"}'
```

```bash
curl -X POST localhost:8080/message \
  -H 'Content-Type: application/json' \
  -d '{"from":"alice","text":"سلام باب"}'
```

```bash
curl localhost:8080/users
```

```bash
curl localhost:8080/messages?limit=10
```

## چرا این الگو actor-modelی است؟

نکته اصلی این است:

```text
users map[string]struct{}
```

فقط داخل `ChatRoomActor` تغییر می‌کند. API مستقیم به این map دست نمی‌زند. Redis handler هم جدا نیست که همزمان state را تغییر بدهد. همه چیز باید از این در رد شود:

```go
node.Call(roomPID, Join{...})
node.Call(roomPID, PostMessage{...})
node.Call(roomPID, ListUsers{})
```

پس actor می‌شود **single writer** برای state اتاق.

## آیا این production-ready کامل است؟

برای یک نمونه آموزشیِ نزدیک به production خوب است، ولی برای production واقعی من این تغییرها را هم اضافه می‌کردم:

```text
1. Supervisor برای ChatRoomActor
2. RoomRegistryActor برای چند room
3. RedisWriterActor یا Pool برای اینکه Redis کند، actor اصلی را stall نکند
4. metrics و logging بهتر
5. request timeout برای node.Call
6. auth/user identity واقعی، نه from داخل body
7. idempotency برای message write
```

اما همین نسخه، اسکلت درست را نشان می‌دهد:

```text
HTTP فقط adapter است
Actor مالک state و rule است
Redis persistence است
Graceful shutdown اول HTTP را می‌بندد، بعد actor/node، بعد Redis
```

[1]: https://docs.ergo.services/basics/process "Process | Ergo Framework documentation"
[2]: https://docs.ergo.services/meta-processes/web "Web | Ergo Framework documentation"
[3]: https://github.com/orgs/ergo-services/discussions/189 "Real World Examples? · ergo-services · Discussion #189 · GitHub"
[4]: https://pkg.go.dev/net/http?utm_source=chatgpt.com "net/http"
[5]: https://pkg.go.dev/os/signal?utm_source=chatgpt.com "signal package - os/signal"