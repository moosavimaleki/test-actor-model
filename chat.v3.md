حق با توست. مثال قبلی از نظر actor-model واقعی فقیر بود؛ چون فقط یک ChatRoomActor ثابت داشت. طراحی درست‌تر برای نرم‌افزاری مثل chat/game/order/session این است:

Web API
↓
RoomRegistryActor  ← فقط route و lifecycle
↓
RoomActor(room-1)
RoomActor(room-2)
RoomActor(room-3)
...
RoomActor(room-N)
یعنی هر room یک actor جدا است؛ با درخواست‌های وب، room ساخته می‌شود، actor بالا می‌آید، وقتی idle شد actor پایین می‌آید، ولی state در Redis می‌ماند و deploy/restart خرابش نمی‌کند.

این دقیقاً شبیه الگوی Elixir است: Registry + DynamicSupervisor + GenServer-per-room. در Elixir، DynamicSupervisor برای childهایی است که از اول معلوم نیستند و on-demand با start_child ساخته می‌شوند؛ خودش با صفر child شروع می‌شود. (Hexdocs) در Ergo هم معادل نزدیکش Simple One For One supervisor است: با صفر child شروع می‌کند، childهای هم‌شکل را پویا start می‌کند، و برای workloadهای dynamic توصیه شده است. (Ergo Framework Documentation)

اما برای persistence یک اصل مهم داریم: state actor باید بیرون actor ذخیره شود. حتی در Akka/Pekko هم actor persistent برای restart/move باید state را در DB/event-log نگه دارد؛ mailbox ذخیره نمی‌شود. (Discussion Forum for Akka.) پس در Go/Ergo هم نباید دنبال freeze کردن goroutine/mailbox باشیم؛ باید actorها hydratable باشند.

سند طراحی نسخه درست‌تر
موجودیت‌ها
RoomRegistryActor
- می‌داند کدام room الان actor فعال دارد.
- requestهای HTTP را به room actor درست route می‌کند.
- اگر room actor وجود نداشت، از Redis hydrate و spawn می‌کند.
- هنگام deploy، همه room actorها را drain می‌کند.

RoomActor
- مالک state یک room است.
- members/messages/meta را فقط خودش تغییر می‌دهد.
- بعد از هر command مهم، snapshot را در Redis می‌نویسد.
- با Init از Redis hydrate می‌شود.

Redis
- durable state
- لیست roomها
- snapshot هر room
  کلیدهای Redis
  chat:rooms
  SET room_idها

chat:room:{roomID}:snapshot
JSON snapshot آخرین state room

chat:room:{roomID}:messages
LIST پیام‌های اخیر
در production قوی‌تر، بهتر است به‌جای snapshot-only از این مدل استفاده کنی:

event log + periodic snapshot
ولی برای نمونه‌ی قابل فهم، اینجا snapshot بعد از هر mutation داریم.

جریان ساخت room
POST /rooms
↓
RoomRegistryActor.CreateRoom
↓
Redis: SADD chat:rooms roomID
Redis: SET snapshot اولیه
↓
Spawn RoomActor(roomID)
↓
RoomActor.Init → hydrate از Redis
جریان join/message
POST /rooms/{id}/join
↓
Registry.ensureRoom(id)
↓
اگر actor زنده نبود:
snapshot از Redis چک می‌شود
RoomActor جدید spawn می‌شود
↓
Call به RoomActor
↓
RoomActor state را تغییر می‌دهد
↓
snapshot در Redis ذخیره می‌کند
جریان deploy امن
SIGTERM
↓
1. HTTP server Shutdown
   یعنی request جدید وارد سیستم نشود.

2. RegistryActor.Drain
   یعنی همه RoomActorها snapshot نهایی بزنند.

3. RoomActorها shutdown شوند.

4. node.Stop()

5. نسخه جدید بالا بیاید.

6. RoomActorها lazy hydrate شوند.
   نکته مهم: Ergo هم processها را sequential پردازش می‌کند؛ callbackها را نباید با goroutine/channel/mutex خراب کنیم، چون خود داک Ergo می‌گوید actor یک پیام در هر لحظه پردازش می‌کند و goroutine زدن داخل callback می‌تواند race روی state actor بسازد. (Ergo Framework Documentation)

نمونه کد جدید
این نمونه کامل production نیست، ولی اسکلت درست را نشان می‌دهد:

dynamic actors
state persistence
lazy hydration
actor unload
graceful deploy drain
HTTP API
Redis
نصب
mkdir ergo-dynamic-chat
cd ergo-dynamic-chat

go mod init ergo-dynamic-chat
go get ergo.services/ergo@latest
go get github.com/redis/go-redis/v9
Redis:

docker run --rm --name chat-redis -p 6379:6379 redis:7-alpine
main.go
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
"strings"
"syscall"
"time"

	"github.com/redis/go-redis/v9"

	ergo "ergo.services/ergo"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

//
// =============================
// Result / Command Types
// =============================
//

type Result struct {
OK    bool   `json:"ok"`
Error string `json:"error,omitempty"`
}

func ok() Result {
return Result{OK: true}
}

func fail(msg string) Result {
return Result{OK: false, Error: msg}
}

type CreateRoom struct {
RoomID string `json:"room_id"`
Title  string `json:"title"`
}

type JoinRoom struct {
RoomID string `json:"room_id,omitempty"`
Nick   string `json:"nick"`
}

type LeaveRoom struct {
RoomID string `json:"room_id,omitempty"`
Nick   string `json:"nick"`
}

type PostMessage struct {
RoomID string `json:"room_id,omitempty"`
From   string `json:"from"`
Text   string `json:"text"`
}

type GetRoom struct {
RoomID string
}

type ListRooms struct{}

type UnloadRoom struct {
RoomID string
}

type DrainRegistry struct{}

type RoomSnapshot struct {
Version   int       `json:"version"`
RoomID    string    `json:"room_id"`
Title     string    `json:"title"`
Members   []string  `json:"members"`
UpdatedAt time.Time `json:"updated_at"`
}

type ChatMessage struct {
From      string    `json:"from"`
Text      string    `json:"text"`
CreatedAt time.Time `json:"created_at"`
}

//
// =============================
// RoomActor
// =============================
//

type RoomConfig struct {
RoomID string
Redis  *redis.Client
}

type RoomActor struct {
act.Actor

	roomID  string
	title   string
	redis   *redis.Client
	members map[string]struct{}
}

func newRoomActor() gen.ProcessBehavior {
return &RoomActor{}
}

// Init یعنی actor تازه بالا آمده.
// اینجا state از Redis hydrate می‌شود.
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
	r.members = make(map[string]struct{})

	if err := r.hydrate(); err != nil {
		return err
	}

	r.Log().Info("room actor started. room_id=%s pid=%s members=%d", r.roomID, r.PID(), len(r.members))

	return nil
}

// HandleCall همه commandهای مربوط به یک room را sequential اجرا می‌کند.
// پس members map نیازی به mutex ندارد.
func (r *RoomActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
switch req := request.(type) {

	case JoinRoom:
		return r.join(req.Nick), nil

	case LeaveRoom:
		return r.leave(req.Nick), nil

	case PostMessage:
		return r.postMessage(req.From, req.Text), nil

	case GetRoom:
		return r.snapshot(), nil

	case DrainRegistry:
		// در shutdown، registry به همه roomها این call را می‌زند.
		// اینجا آخرین snapshot را durable می‌کنیم.
		if err := r.persistSnapshot(); err != nil {
			return fail(err.Error()), nil
		}
		return ok(), nil

	default:
		return fail(fmt.Sprintf("unknown room request: %T", request)), nil
	}
}

func (r *RoomActor) Terminate(reason error) {
// در Terminate نباید کار سنگین و طولانی بکنی.
// snapshot اصلی باید قبلش در Drain انجام شده باشد.
r.Log().Info("room actor stopped. room_id=%s reason=%v", r.roomID, reason)
}

func (r *RoomActor) join(nick string) Result {
nick = strings.TrimSpace(nick)
if nick == "" {
return fail("nick is empty")
}

	r.members[nick] = struct{}{}

	if err := r.persistSnapshot(); err != nil {
		return fail("snapshot failed: " + err.Error())
	}

	return ok()
}

func (r *RoomActor) leave(nick string) Result {
nick = strings.TrimSpace(nick)
if nick == "" {
return fail("nick is empty")
}

	delete(r.members, nick)

	if err := r.persistSnapshot(); err != nil {
		return fail("snapshot failed: " + err.Error())
	}

	return ok()
}

func (r *RoomActor) postMessage(from, text string) Result {
from = strings.TrimSpace(from)
text = strings.TrimSpace(text)

	if from == "" {
		return fail("from is empty")
	}
	if text == "" {
		return fail("text is empty")
	}
	if _, exists := r.members[from]; !exists {
		return fail("sender is not member of room")
	}

	msg := ChatMessage{
		From:      from,
		Text:      text,
		CreatedAt: time.Now().UTC(),
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fail(err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pipe := r.redis.TxPipeline()

	// پیام‌ها durable می‌شوند.
	pipe.RPush(ctx, r.messagesKey(), payload)

	// فقط آخرین 100 پیام را نگه می‌داریم.
	pipe.LTrim(ctx, r.messagesKey(), -100, -1)

	// snapshot هم update می‌شود.
	snap := r.snapshot()
	snapPayload, _ := json.Marshal(snap)
	pipe.Set(ctx, r.snapshotKey(), snapPayload, 0)

	if _, err := pipe.Exec(ctx); err != nil {
		return fail("redis write failed: " + err.Error())
	}

	return ok()
}

func (r *RoomActor) hydrate() error {
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

	raw, err := r.redis.Get(ctx, r.snapshotKey()).Result()
	if err != nil {
		return fmt.Errorf("room snapshot not found: %w", err)
	}

	var snap RoomSnapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		return err
	}

	if snap.Version != 1 {
		return fmt.Errorf("unsupported room snapshot version: %d", snap.Version)
	}

	r.title = snap.Title
	r.members = make(map[string]struct{}, len(snap.Members))

	for _, nick := range snap.Members {
		if nick != "" {
			r.members[nick] = struct{}{}
		}
	}

	return nil
}

func (r *RoomActor) snapshot() RoomSnapshot {
members := make([]string, 0, len(r.members))

	for nick := range r.members {
		members = append(members, nick)
	}

	return RoomSnapshot{
		Version:   1,
		RoomID:    r.roomID,
		Title:     r.title,
		Members:   members,
		UpdatedAt: time.Now().UTC(),
	}
}

func (r *RoomActor) persistSnapshot() error {
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

	payload, err := json.Marshal(r.snapshot())
	if err != nil {
		return err
	}

	return r.redis.Set(ctx, r.snapshotKey(), payload, 0).Err()
}

func (r *RoomActor) snapshotKey() string {
return "chat:room:" + r.roomID + ":snapshot"
}

func (r *RoomActor) messagesKey() string {
return "chat:room:" + r.roomID + ":messages"
}

//
// =============================
// RoomRegistryActor
// =============================
//

type RegistryConfig struct {
Redis *redis.Client
}

type RoomRegistryActor struct {
act.Actor

	redis    *redis.Client
	rooms    map[string]gen.PID
	draining bool
}

func newRoomRegistryActor() gen.ProcessBehavior {
return &RoomRegistryActor{}
}

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

// Registry خودش state همه roomها را نگه نمی‌دارد.
// فقط PIDهای فعال را نگه می‌دارد و route می‌کند.
func (rg *RoomRegistryActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
if rg.draining {
switch request.(type) {
case DrainRegistry:
// اجازه بده drain خودش اجرا شود.
default:
return fail("server is draining"), nil
}
}

	switch req := request.(type) {

	case CreateRoom:
		return rg.createRoom(req), nil

	case JoinRoom:
		pid, res := rg.ensureRoom(req.RoomID)
		if !res.OK {
			return res, nil
		}
		return rg.Call(pid, JoinRoom{Nick: req.Nick})

	case LeaveRoom:
		pid, res := rg.ensureRoom(req.RoomID)
		if !res.OK {
			return res, nil
		}
		return rg.Call(pid, LeaveRoom{Nick: req.Nick})

	case PostMessage:
		pid, res := rg.ensureRoom(req.RoomID)
		if !res.OK {
			return res, nil
		}
		return rg.Call(pid, PostMessage{From: req.From, Text: req.Text})

	case GetRoom:
		pid, res := rg.ensureRoom(req.RoomID)
		if !res.OK {
			return res, nil
		}
		return rg.Call(pid, req)

	case ListRooms:
		return rg.listRooms(), nil

	case UnloadRoom:
		return rg.unloadRoom(req.RoomID), nil

	case DrainRegistry:
		return rg.drainAllRooms(), nil

	default:
		return fail(fmt.Sprintf("unknown registry request: %T", request)), nil
	}
}

func (rg *RoomRegistryActor) createRoom(req CreateRoom) Result {
roomID := strings.TrimSpace(req.RoomID)
title := strings.TrimSpace(req.Title)

	if roomID == "" {
		return fail("room_id is empty")
	}
	if title == "" {
		title = roomID
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	snapshotKey := "chat:room:" + roomID + ":snapshot"

	exists, err := rg.redis.Exists(ctx, snapshotKey).Result()
	if err != nil {
		return fail("redis exists failed: " + err.Error())
	}
	if exists > 0 {
		// اگر snapshot هست، room قبلاً وجود دارد.
		// فقط مطمئن می‌شویم actor آن بالا آمده.
		_, res := rg.ensureRoom(roomID)
		return res
	}

	snap := RoomSnapshot{
		Version:   1,
		RoomID:    roomID,
		Title:     title,
		Members:   []string{},
		UpdatedAt: time.Now().UTC(),
	}

	payload, err := json.Marshal(snap)
	if err != nil {
		return fail(err.Error())
	}

	pipe := rg.redis.TxPipeline()
	pipe.SAdd(ctx, "chat:rooms", roomID)
	pipe.Set(ctx, snapshotKey, payload, 0)

	if _, err := pipe.Exec(ctx); err != nil {
		return fail("redis create room failed: " + err.Error())
	}

	_, res := rg.ensureRoom(roomID)
	return res
}

// ensureRoom قلب dynamic actor system است.
//
// اگر room actor الان فعال است، PID همان را می‌دهد.
// اگر فعال نیست ولی snapshot در Redis وجود دارد، actor جدید spawn می‌کند.
// یعنی actorها با requestهای وب کم و زیاد می‌شوند.
func (rg *RoomRegistryActor) ensureRoom(roomID string) (gen.PID, Result) {
roomID = strings.TrimSpace(roomID)
if roomID == "" {
return gen.PID{}, fail("room_id is empty")
}

	if pid, exists := rg.rooms[roomID]; exists {
		return pid, ok()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	exists, err := rg.redis.Exists(ctx, "chat:room:"+roomID+":snapshot").Result()
	if err != nil {
		return gen.PID{}, fail("redis exists failed: " + err.Error())
	}
	if exists == 0 {
		return gen.PID{}, fail("room not found")
	}

	// اینجا actor جدید ساخته می‌شود.
	// نکته: هر room یک actor مستقل دارد.
	pid, err := rg.Spawn(
		newRoomActor,
		gen.ProcessOptions{},
		RoomConfig{
			RoomID: roomID,
			Redis:  rg.redis,
		},
	)
	if err != nil {
		return gen.PID{}, fail("spawn room failed: " + err.Error())
	}

	rg.rooms[roomID] = pid

	rg.Log().Info("room actor loaded. room_id=%s pid=%s active_rooms=%d", roomID, pid, len(rg.rooms))

	return pid, ok()
}

func (rg *RoomRegistryActor) unloadRoom(roomID string) Result {
pid, exists := rg.rooms[roomID]
if !exists {
return ok()
}

	// قبل از unload، snapshot نهایی.
	resAny, err := rg.Call(pid, DrainRegistry{})
	if err != nil {
		return fail("room drain failed: " + err.Error())
	}

	if res, ok := resAny.(Result); ok && !res.OK {
		return res
	}

	// actor را پایین می‌آوریم؛ Redis state باقی می‌ماند.
	_ = rg.SendExit(pid, gen.TerminateReasonShutdown)

	delete(rg.rooms, roomID)

	rg.Log().Info("room actor unloaded. room_id=%s active_rooms=%d", roomID, len(rg.rooms))

	return ok()
}

func (rg *RoomRegistryActor) listRooms() any {
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

	rooms, err := rg.redis.SMembers(ctx, "chat:rooms").Result()
	if err != nil {
		return fail("redis list rooms failed: " + err.Error())
	}

	return map[string]any{
		"rooms":        rooms,
		"active_count": len(rg.rooms),
	}
}

func (rg *RoomRegistryActor) drainAllRooms() Result {
rg.draining = true

	for roomID, pid := range rg.rooms {
		resAny, err := rg.Call(pid, DrainRegistry{})
		if err != nil {
			return fail("drain room " + roomID + " failed: " + err.Error())
		}

		if res, ok := resAny.(Result); ok && !res.OK {
			return fail("drain room " + roomID + " failed: " + res.Error)
		}
	}

	for roomID, pid := range rg.rooms {
		_ = rg.SendExit(pid, gen.TerminateReasonShutdown)
		delete(rg.rooms, roomID)
	}

	return ok()
}

func (rg *RoomRegistryActor) Terminate(reason error) {
rg.Log().Info("room registry stopped. reason=%v", reason)
}

//
// =============================
// HTTP API
// =============================
//

type API struct {
node        gen.Node
registryPID gen.PID
}

func newHTTPHandler(node gen.Node, registryPID gen.PID) http.Handler {
api := &API{
node:        node,
registryPID: registryPID,
}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", api.healthz)
	mux.HandleFunc("GET /rooms", api.listRooms)
	mux.HandleFunc("POST /rooms", api.createRoom)

	// ساده نگه داشتیم: path را دستی parse می‌کنیم.
	mux.HandleFunc("/rooms/", api.roomSubroutes)

	return jsonMiddleware(mux)
}

func (api *API) healthz(w http.ResponseWriter, r *http.Request) {
writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (api *API) createRoom(w http.ResponseWriter, r *http.Request) {
var req CreateRoom
if !decodeJSON(w, r, &req) {
return
}

	resAny, err := api.node.Call(api.registryPID, req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, fail(err.Error()))
		return
	}

	writeActorResult(w, resAny)
}

func (api *API) listRooms(w http.ResponseWriter, r *http.Request) {
resAny, err := api.node.Call(api.registryPID, ListRooms{})
if err != nil {
writeJSON(w, http.StatusInternalServerError, fail(err.Error()))
return
}

	writeJSON(w, http.StatusOK, resAny)
}

func (api *API) roomSubroutes(w http.ResponseWriter, r *http.Request) {
// /rooms/{roomID}
// /rooms/{roomID}/join
// /rooms/{roomID}/leave
// /rooms/{roomID}/message
// /rooms/{roomID}/unload

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/rooms/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeJSON(w, http.StatusNotFound, fail("room_id is required"))
		return
	}

	roomID := parts[0]

	if len(parts) == 1 && r.Method == http.MethodGet {
		resAny, err := api.node.Call(api.registryPID, GetRoom{RoomID: roomID})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, fail(err.Error()))
			return
		}
		writeJSON(w, http.StatusOK, resAny)
		return
	}

	if len(parts) != 2 {
		writeJSON(w, http.StatusNotFound, fail("not found"))
		return
	}

	switch parts[1] {

	case "join":
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, fail("method not allowed"))
			return
		}

		var req JoinRoom
		if !decodeJSON(w, r, &req) {
			return
		}
		req.RoomID = roomID

		resAny, err := api.node.Call(api.registryPID, req)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, fail(err.Error()))
			return
		}
		writeActorResult(w, resAny)

	case "leave":
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, fail("method not allowed"))
			return
		}

		var req LeaveRoom
		if !decodeJSON(w, r, &req) {
			return
		}
		req.RoomID = roomID

		resAny, err := api.node.Call(api.registryPID, req)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, fail(err.Error()))
			return
		}
		writeActorResult(w, resAny)

	case "message":
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, fail("method not allowed"))
			return
		}

		var req PostMessage
		if !decodeJSON(w, r, &req) {
			return
		}
		req.RoomID = roomID

		resAny, err := api.node.Call(api.registryPID, req)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, fail(err.Error()))
			return
		}
		writeActorResult(w, resAny)

	case "unload":
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, fail("method not allowed"))
			return
		}

		resAny, err := api.node.Call(api.registryPID, UnloadRoom{RoomID: roomID})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, fail(err.Error()))
			return
		}
		writeActorResult(w, resAny)

	default:
		writeJSON(w, http.StatusNotFound, fail("not found"))
	}
}

//
// =============================
// Helpers
// =============================
//

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, fail("invalid json: "+err.Error()))
		return false
	}

	return true
}

func writeActorResult(w http.ResponseWriter, resAny any) {
if res, ok := resAny.(Result); ok {
if !res.OK {
writeJSON(w, http.StatusBadRequest, res)
return
}
writeJSON(w, http.StatusOK, res)
return
}

	writeJSON(w, http.StatusOK, resAny)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
w.WriteHeader(status)
_ = json.NewEncoder(w).Encode(body)
}

func jsonMiddleware(next http.Handler) http.Handler {
return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json; charset=utf-8")
next.ServeHTTP(w, r)
})
}

//
// =============================
// main + graceful deploy
// =============================
//

func main() {
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("redis not ready:", err)
	}

	node, err := ergo.StartNode("chat@localhost", gen.NodeOptions{})
	if err != nil {
		log.Fatal(err)
	}

	registryPID, err := node.Spawn(
		newRoomRegistryActor,
		gen.ProcessOptions{},
		RegistryConfig{Redis: rdb},
	)
	if err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:              ":8080",
		Handler:           newHTTPHandler(node, registryPID),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)

	go func() {
		log.Println("HTTP listening on :8080")
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case <-ctx.Done():
		log.Println("shutdown signal received")

	case err := <-serverErr:
		if err != nil {
			log.Fatal(err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. اول HTTP را می‌بندیم تا request جدید وارد actorها نشود.
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Println("http shutdown error:", err)
	} else {
		log.Println("http stopped")
	}

	// 2. بعد همه actorهای dynamic را drain می‌کنیم.
	resAny, err := node.Call(registryPID, DrainRegistry{})
	if err != nil {
		log.Println("registry drain error:", err)
	} else if res, ok := resAny.(Result); ok && !res.OK {
		log.Println("registry drain failed:", res.Error)
	} else {
		log.Println("registry drained")
	}

	// 3. بعد node را stop می‌کنیم.
	node.Stop()
	log.Println("ergo node stopped")

	// 4. بعد Redis client را می‌بندیم.
	_ = rdb.Close()
	log.Println("redis closed")
}
تست
go run .
ساخت room:

curl -X POST localhost:8080/rooms \
-H 'Content-Type: application/json' \
-d '{"room_id":"room-1","title":"General"}'
join:

curl -X POST localhost:8080/rooms/room-1/join \
-H 'Content-Type: application/json' \
-d '{"nick":"alice"}'
message:

curl -X POST localhost:8080/rooms/room-1/message \
-H 'Content-Type: application/json' \
-d '{"from":"alice","text":"hello"}'
دیدن state:

curl localhost:8080/rooms/room-1
ساخت room دوم و سوم:

curl -X POST localhost:8080/rooms \
-H 'Content-Type: application/json' \
-d '{"room_id":"room-2","title":"Tech"}'

curl -X POST localhost:8080/rooms \
-H 'Content-Type: application/json' \
-d '{"room_id":"room-3","title":"Random"}'
لیست roomها:

curl localhost:8080/rooms
unload کردن actor بدون حذف state:

curl -X POST localhost:8080/rooms/room-1/unload
بعد دوباره:

curl localhost:8080/rooms/room-1
اینجا RoomActor(room-1) دوباره ساخته می‌شود و از Redis hydrate می‌شود.

چرا این طراحی از قبلی درست‌تر است؟
چون قبلی این بود:

یک actor ثابت برای کل chat
ولی این یکی:

RoomRegistryActor
├── RoomActor(room-1)
├── RoomActor(room-2)
├── RoomActor(room-3)
└── ...
یعنی actorها با درخواست وب زیاد و کم می‌شوند.

در داک Ergo هم processها سبک معرفی شده‌اند و هرکدام mailbox دارند و پیام‌ها را یکی‌یکی پردازش می‌کنند؛ همین باعث می‌شود هزاران process/actor منطقی قابل تصور باشد، البته با محدودیت memory/mailbox و طراحی درست. (Ergo Framework Documentation)

نکته مهم درباره Supervisor
کد بالا برای اینکه ساده و قابل فهم بماند، RoomRegistryActor خودش Spawn می‌کند و PIDها را نگه می‌دارد. این برای یادگیری خوب است.

اما نسخه production بهتر این است:

RootSupervisor
├── RoomRegistryActor
└── RoomDynamicSupervisor
├── RoomActor(room-1)
├── RoomActor(room-2)
└── ...
در Ergo برای این بخش باید از act.SupervisorTypeSimpleOneForOne استفاده کنی، چون داک Ergo می‌گوید این حالت برای childهای هم‌شکل و dynamic است که on-demand ساخته می‌شوند. (Ergo Framework Documentation)

پس نسخه production‌تر:

type RoomSupervisor struct {
act.Supervisor
}

func newRoomSupervisor() gen.ProcessBehavior {
return &RoomSupervisor{}
}

func (s *RoomSupervisor) Init(args ...any) (act.SupervisorSpec, error) {
return act.SupervisorSpec{
Type: act.SupervisorTypeSimpleOneForOne,
Children: []act.SupervisorChildSpec{
{
Name:    "room",
Factory: newRoomActor,
},
},
Restart: act.SupervisorRestart{
Strategy:  act.SupervisorStrategyTransient,
Intensity: 5,
Period:    10,
},
}, nil
}
ولی چون ارتباط RegistryActor با RoomSupervisor.StartChild و گرفتن PID در نسخه‌های مختلف Ergo ممکن است کمی با API دقیق نسخه فرق کند، در نمونه اصلی از rg.Spawn(...) استفاده کردم که مستقیم و روشن است. اصل معماری عوض نمی‌شود:

Registry مسئول routing و lifecycle است.
RoomActor مسئول state خودش است.
Redis مسئول durability است.
نکته deploy
این سیستم در deploy جدید این‌طوری safe می‌ماند:

قبل از stop:
- HTTP بسته می‌شود.
- request جدید وارد نمی‌شود.
- Registry به همه RoomActorها Drain می‌زند.
- RoomActorها snapshot را در Redis می‌نویسند.
- actorها shutdown می‌شوند.

بعد از start نسخه جدید:
- Registry خالی است.
- با اولین request به هر room، actor دوباره ساخته می‌شود.
- actor از Redis hydrate می‌شود.
  پس چیزی که حفظ می‌شود:

room state
members
messages
room list
چیزی که عمداً حفظ نمی‌شود:

mailbox
goroutine
PID قدیمی
stack
connectionهای قبلی
این همان مرز درست actor persistence است. PID یک آدرس runtime است؛ بعد از restart نباید روی PID قبلی حساب کنی. برای همین Registry باید با roomID کار کند، نه اینکه کل سیستم PID را durable کند.

حرف اصلی:

در actor model جدی، actor زیاد و dynamic است؛
ولی identity پایدار actor، PID نیست.
identity پایدار، domain id است: roomID, userID, orderID, messageID.
برای پروژه خودت هم دقیقاً همین است:

ReactionShardActor(shardID)
MessageActor(messageID)
VendorActor(vendorID)
OrderActor(orderID)
نه اینکه یک actor غول برای همه چیز بسازی.


------------------------------

# بهتر با سوکت

آره، برای یک Chat واقعی باید WebSocket هم داشته باشد؛ ولی نه اینکه socket را داخل خود RoomActor پیاده کنیم.

طراحی درست این است:

WebSocketConnectionActor  ← هر اتصال کاربر، یک actor/meta-process
↓
RoomRegistryActor         ← پیدا/ساختن RoomActor
↓
RoomActor(roomID)         ← state اتاق و broadcast
یعنی:

RoomActor نباید خودش socket بخواند/بنویسد.
RoomActor فقط state و قوانین room را مدیریت کند.
Socket actor فقط connection زنده‌ی کاربر را مدیریت کند.
دلیلش هم دقیقاً در داک Ergo آمده: WebSocket long-lived و full-duplex است؛ باید همزمان از client بخواند و از actorهای backend پیام دریافت کند و به client بنویسد. Ergo برای همین WebSocket meta-process دارد؛ هر connection تبدیل به یک actor/meta-process مستقل و قابل address شدن در کل cluster می‌شود. (Ergo Framework Documentation)

در نمونه‌های واقعی چه کار می‌کنند؟
۱. در Ergo
در repo رسمی ergo-services/examples یک مثال websocket هست که Web server را با websocket-handler بالا می‌آورد و برای connectionها process جدا spawn می‌کند. یعنی همان الگو: connectionها actor/process جدا هستند، نه اینکه همه چیز داخل room actor باشد. (GitHub)

Maintainer Ergo هم در Discussion گفته meta-process برای چیزهایی ساخته شده که نیاز دارند read/write handler زنده بماند؛ مثل TCP socket، درحالی‌که actor پیام‌ها را sequential هندل می‌کند. پس برای DB لازم نیست meta-process بسازی، ولی برای socket منطقی است. (GitHub)

پس در Ergo واقعی‌تر باید این را داشته باشی:

WebSocketHandler
├── WSConnectionActor(client-1)
├── WSConnectionActor(client-2)
├── WSConnectionActor(client-3)
└── ...

RoomRegistryActor
├── RoomActor(room-1)
├── RoomActor(room-2)
└── ...
۲. در Elixir/Phoenix
در Phoenix هم socket/channel جدا از state اصلی room/game است. Client به یک WebSocket endpoint وصل می‌شود، بعد به topicها join می‌کند. پیام‌ها bidirectional هستند و client می‌تواند چند topic را روی همان socket join کند. Phoenix حتی message format، phx_join، phx_leave و heartbeat دارد. (Hexdocs)

در مثال‌های Elixir برای game/chat هم معمولاً state اصلی را از socket جدا می‌کنند. مثلاً در یک نمونه multiplayer game، مقاله می‌گوید اگر state داخل socket connection بماند، player دوم نمی‌تواند به همان game state دسترسی داشته باشد؛ پس برای هر game یک GenServer جدا زیر DynamicSupervisor می‌سازند. (AppSignal Blog)

یعنی الگوی واقعی:

Socket/Channel process = اتصال کاربر
Game/Room GenServer    = state مشترک دامنه
DynamicSupervisor      = ساخت actorهای پویا
Registry               = پیدا کردن actor با domain id
نه اینکه socket خودش state اصلی بازی/room باشد.

پس نسخه درست معماری ما باید این باشد
Client Browser
│ WebSocket
▼
WSConnectionActor
│ join_room / leave_room / message
▼
RoomRegistryActor
│ ensure room actor exists
▼
RoomActor(roomID)
│ persist snapshot/messages
▼
Redis
و برای push کردن پیام به کاربران:

RoomActor(roomID)
│ RoomEvent
├──> WSConnectionActor(alice)
├──> WSConnectionActor(bob)
└──> WSConnectionActor(sara)
مالکیت state کجاست؟
این خیلی مهم است:

WSConnectionActor:
- connection زنده
- userID/nick
- roomهایی که join کرده
- auth/session info
- نوشتن frame به socket

RoomActor:
- members
- پیام‌ها
- قوانین room
- broadcast
- persistence

RoomRegistryActor:
- map roomID → pid
- ساخت room actor
- unload/drain room actorها
  هنگام disconnect چه می‌شود؟
  وقتی socket قطع شد:

WSConnectionActor terminate می‌شود
↓
به RoomRegistry/RoomActor خبر می‌دهد:
LeaveConnection{connectionID, rooms}
↓
RoomActor member را حذف می‌کند
↓
snapshot را در Redis ذخیره می‌کند
↓
به بقیه broadcast می‌کند
Connection actorها ephemeral هستند. یعنی بعد از deploy/restart لازم نیست همان socketها زنده بمانند. Browser/client reconnect می‌کند و دوباره join می‌زند.

چیزی که persistent است:

room state
members اگر واقعاً لازم باشد
messages
room list
user session token
چیزی که persistent نیست:

WebSocket TCP connection
connection actor pid
mailbox
goroutine
جواب مستقیم به سؤال تو
بله، اگر قرار بود مثال واقعاً chat باشد، باید WebSocket هم اضافه می‌کردم. اما نه داخل همان RoomActor.

نسخه درست‌تر باید سه actor اصلی داشته باشد:

1. RoomRegistryActor
   ساخت و پیدا کردن room actorها

2. RoomActor
   state هر room + Redis persistence + broadcast

3. WSConnectionActor یا Ergo WebSocket meta-process
   هر اتصال کاربر + read/write socket + forward به actorها
   و دقیقاً این تفکیک مهم است:

Socket actor = ارتباط زنده با client
Room actor   = state دامنه
Registry     = lifecycle و routing
Redis        = durability
پس اگر بخواهیم ادامه بدهیم، مرحله بعدی باید این باشد: همان پروژه قبلی را با WebSocketConnectionActor کامل کنیم، یعنی پیام‌های join, leave, message, ping, room_event را از طریق WebSocket رد و بدل کنیم.

