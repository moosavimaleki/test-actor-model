# Chat Demo v3 - Actorهای پویا + WebSocket

این نسخه دیگر یک actor ثابت برای کل chat ندارد.

معماری فعلی:

```text
HTTP API / WebSocket
        |
        v
RoomRegistryActor
        |
        +--> RoomActor(room-1)
        +--> RoomActor(room-2)
        +--> RoomActor(room-3)

WebSocket client
        |
        v
WSConnectionActor(conn-1)
```

سه actor کاربردی داریم:

1. `RoomRegistryActor`
   مسئول پیدا کردن، ساختن، unload کردن و drain کردن room actorهاست.

2. `RoomActor`
   هر room یک actor جدا دارد. state همان room فقط داخل همین actor تغییر می‌کند.

3. `WSConnectionActor`
   هر WebSocket connection یک actor جدا دارد. این actor اتصال زنده کاربر را مدیریت می‌کند.

## چرا این نسخه واقعی‌تر است؟

در chat واقعی، همه roomها نباید داخل یک actor بزرگ باشند.

اگر یک actor برای همه roomها داشته باشی:

- همه پیام‌ها از یک mailbox رد می‌شوند
- یک room شلوغ می‌تواند roomهای دیگر را عقب بیندازد
- lifecycle هر room جدا نیست
- unload و hydrate جداگانه سخت می‌شود

در v3 هر room actor مستقل دارد. پس اگر `room-1` شلوغ باشد، state و mailbox خودش را دارد.

## identity پایدار چیست؟

در actor system، `PID` پایدار نیست.

`PID` آدرس runtime actor است. با restart، deploy یا unload عوض می‌شود.

چیزی که پایدار است `roomID` است:

```text
roomID = identity دامنه
PID    = آدرس runtime
```

برای همین Redis با `roomID` کار می‌کند، نه با `PID`.

## نقش Redis

Redis در این تمرین storage پایدار است.

کلیدها:

```text
chat:rooms
chat:room:{roomID}:snapshot
chat:room:{roomID}:messages
```

`chat:rooms` لیست roomهاست.

`snapshot` آخرین state قابل hydrate شدن room را نگه می‌دارد.

`messages` آخرین پیام‌های room را نگه می‌دارد.

در production قوی‌تر معمولاً بهتر است `event log + snapshot دوره‌ای` داشته باشی.
ولی برای تمرین، snapshot بعد از mutation کافی و قابل فهم است.

## RoomRegistryActor چه کار می‌کند؟

Registry خودش owner state room نیست.

کارهایش:

- اگر room وجود نداشت، آن را از Redis چک می‌کند
- اگر snapshot وجود داشت، `RoomActor` جدید spawn می‌کند
- `roomID -> PID` actorهای فعال را در memory نگه می‌دارد
- commandهای HTTP و WebSocket را به room درست route می‌کند
- هنگام shutdown همه room actorها را drain می‌کند

فایل‌های مربوط:

- `internal/chat/registry_actor.go`
- `internal/chat/registry_rooms.go`
- `internal/chat/registry_route.go`
- `internal/chat/registry_drain.go`

## RoomActor چه کار می‌کند؟

هر `RoomActor` فقط state یک room را نگه می‌دارد:

```text
members map[string]roomMember
```

چرا mutex ندارد؟

چون Ergo پیام‌های actor را sequential پردازش می‌کند.
یعنی دو command همزمان داخل `RoomActor` اجرا نمی‌شوند.

پس این map فقط توسط خود actor تغییر می‌کند.

کارهای RoomActor:

- join
- leave
- post message
- list messages
- ساخت snapshot
- persist کردن snapshot و messageها در Redis
- broadcast کردن event به connection actorهای زنده

فایل‌های مربوط:

- `internal/chat/room_actor.go`
- `internal/chat/room_members.go`
- `internal/chat/room_messages.go`
- `internal/chat/room_storage.go`

## WSConnectionActor چه کار می‌کند؟

WebSocket یک connection زنده و long-lived است.

این state با state room فرق دارد.

`WSConnectionActor` مالک این چیزهاست:

- connection فعلی
- roomهایی که همین socket join کرده
- نوشتن eventها به client
- تبدیل frameهای WebSocket به commandهای actor system

RoomActor نباید خودش socket را بخواند یا بنویسد.
RoomActor فقط state دامنه و broadcast event را مدیریت می‌کند.

فایل‌های مربوط:

- `internal/wsconn/actor.go`
- `internal/wsconn/io.go`
- `internal/wsconn/client_commands.go`
- `internal/wsconn/client_queries.go`

## چرا در WSConnectionActor goroutine داریم؟

خود actor state را sequential تغییر می‌دهد.
اما WebSocket I/O ذاتاً blocking است.

پس دو goroutine داریم:

- read loop
- write loop

نکته مهم این است:

این goroutineها state actor را مستقیم تغییر نمی‌دهند.

read loop فقط frame را می‌خواند و به mailbox actor پیام می‌فرستد.
write loop فقط frameهای خروجی را از channel می‌گیرد و روی socket می‌نویسد.

پس قانون actor model حفظ می‌شود:

```text
state فقط داخل callback actor تغییر می‌کند
```

## مسیر join از WebSocket

Client این frame را می‌فرستد:

```json
{"type":"join","room_id":"room-1","nick":"alice"}
```

مسیر اجرا:

```text
WSConnectionActor
  -> RoomRegistryActor
  -> ensure RoomActor(room-1)
  -> RoomActor.join(...)
  -> Redis snapshot
  -> broadcast join event
```

## مسیر message از WebSocket

Client:

```json
{"type":"message","room_id":"room-1","text":"hello"}
```

اگر قبلاً join کرده باشد، nick از state همان connection actor خوانده می‌شود.

بعد:

```text
WSConnectionActor
  -> RoomRegistryActor
  -> RoomActor(room-1)
  -> validate sender is member
  -> Redis RPUSH message
  -> Redis SET snapshot
  -> broadcast RoomEvent to live WSConnectionActorها
```

## مسیر HTTP

HTTP هنوز وجود دارد، چون برای تست و admin ساده مفید است:

```text
POST /rooms
GET  /rooms
GET  /rooms/{roomID}
POST /rooms/{roomID}/join
POST /rooms/{roomID}/leave
POST /rooms/{roomID}/message
GET  /rooms/{roomID}/messages
POST /rooms/{roomID}/unload
GET  /ws
```

HTTP handler خودش business state ندارد.
فقط JSON را parse می‌کند و به Registry actor `Call` می‌زند.

## unload یعنی چه؟

این endpoint:

```text
POST /rooms/{roomID}/unload
```

actor آن room را پایین می‌آورد، ولی Redis state را حذف نمی‌کند.

پس اگر دوباره به همان room request بزنی:

```text
RegistryActor.ensureRoom
  -> Redis snapshot exists
  -> spawn RoomActor
  -> hydrate from Redis
```

این دقیقاً همان ایده actorهای dynamic و hydratable است.

## graceful shutdown

ترتیب خاموش شدن مهم است:

1. HTTP server shutdown می‌شود تا request جدید وارد نشود.
2. Registry actor همه room actorها را drain می‌کند.
3. هر room actor snapshot نهایی را Redis می‌نویسد.
4. node stop می‌شود.
5. Redis client بسته می‌شود.

این یعنی deploy امن‌تر:

```text
PIDها از بین می‌روند
ولی roomID و snapshot در Redis می‌مانند
```

## Build

```bash
cd /home/h-mousavi/Projects/Hamed/actor/chatdemo
go build ./...
```

## Run

Redis:

```bash
docker run --rm --name ergo-chat-redis -p 6379:6379 redis:7-alpine
```

برنامه:

```bash
go run ./cmd/chatdemo
```

## تست HTTP

ساخت room:

```bash
curl -X POST localhost:8080/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id":"room-1","title":"General"}'
```

join:

```bash
curl -X POST localhost:8080/rooms/room-1/join \
  -H 'Content-Type: application/json' \
  -d '{"nick":"alice"}'
```

message:

```bash
curl -X POST localhost:8080/rooms/room-1/message \
  -H 'Content-Type: application/json' \
  -d '{"from":"alice","text":"hello"}'
```

دیدن state:

```bash
curl localhost:8080/rooms/room-1
```

دیدن پیام‌ها:

```bash
curl 'localhost:8080/rooms/room-1/messages?limit=10'
```

unload:

```bash
curl -X POST localhost:8080/rooms/room-1/unload
```

## تست WebSocket

اگر `websocat` داری:

```bash
websocat ws://localhost:8080/ws
```

بعد داخل اتصال:

```json
{"type":"join","room_id":"room-1","nick":"alice"}
```

```json
{"type":"message","room_id":"room-1","text":"hello from socket"}
```

```json
{"type":"history","room_id":"room-1","limit":10}
```

برای دیدن broadcast واقعی، دو terminal باز کن و هر دو به `/ws` وصل شو.
وقتی هر دو join کنند، پیام یکی برای دیگری هم push می‌شود.

## جمع‌بندی ذهنی

این نسخه را این‌طور بخوان:

```text
Registry = پیدا کردن و ساختن actorها
RoomActor = state و rule هر room
WSConnectionActor = اتصال زنده کاربر
Redis = durability
```

در actor model جدی، معمولاً یک actor بزرگ برای همه‌چیز نمی‌سازی.
actorها را با identity دامنه می‌سازی:

```text
RoomActor(roomID)
OrderActor(orderID)
SessionActor(sessionID)
ShardActor(shardID)
```

این همان الگوی قابل استفاده در سیستم‌های واقعی‌تر است.
