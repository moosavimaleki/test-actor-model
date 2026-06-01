# راهنمای فارسی Load Test

این پروژه الان یک ابزار load test جدا دارد:

```bash
go run ./cmd/loadtest
```

قبل از اجرای loadtest باید خود chat server بالا باشد.
در دو terminal جدا:

```bash
docker run --rm --name ergo-chat-redis -p 6379:6379 redis:7-alpine
```

```bash
go run ./cmd/chatdemo
```

بعد در terminal سوم loadtest را اجرا کن.
ابزار loadtest قبل از شروع، `/healthz` را چک می‌کند و اگر server بالا نباشد سریع fail می‌کند.

## معنی عددهای تو

تو گفتی:

```text
1,000,000 room
2 یا 3 نفر در هر room
هر room هر 20 ثانیه یک پیام
```

پس نرخ پیام می‌شود:

```text
1,000,000 / 20 = 50,000 message request per second
```

اما اگر واقعاً WebSocket هم برای همه userها باز باشد، یعنی:

```text
2,000,000 تا 3,000,000 WebSocket connection
```

این را با یک ماشین و یک process نباید تست کرد. این یک تست distributed است.

## دو فاز تست

فاز setup:

```bash
go run ./cmd/loadtest \
  -phase setup \
  -base-url http://localhost:8080 \
  -room-start 0 \
  -rooms 1000000 \
  -users-per-room 3 \
  -workers 1024
```

این فاز:

- roomها را می‌سازد
- userها را join می‌کند
- actorهای room را فعال می‌کند

در ابزار فعلی، setup عمداً دو مرحله‌ای است:

```text
اول create همه roomها
بعد join همه userها
```

اگر create و join برای یک room همزمان و بی‌ترتیب اجرا شوند، join ممکن است قبل از snapshot اولیه room برسد و خطای `room not found` بدهد.

فاز messages:

```bash
go run ./cmd/loadtest \
  -phase messages \
  -base-url http://localhost:8080 \
  -room-start 0 \
  -rooms 1000000 \
  -users-per-room 3 \
  -rps 50000 \
  -duration 10m \
  -workers 2048
```

این فاز پیام تصادفی بین roomها پخش می‌کند.

اگر فقط `-phase messages` بزنی، باید قبلش همان range را setup کرده باشی.
مثلاً این غلط است:

```bash
go run ./cmd/loadtest -phase messages -rooms 5000
```

مگر اینکه قبلاً این را زده باشی:

```bash
go run ./cmd/loadtest -phase setup -rooms 5000 -users-per-room 3
```

اگر setup انجام نشده باشد، preflight سریع خطا می‌دهد و دیگر هزاران request بی‌فایده نمی‌فرستد.

نسخه فعلی به صورت پیش‌فرض `auto-setup` هم دارد.
یعنی اگر در فاز messages یک room یا member آماده نباشد، loadtest همان room را می‌سازد، userها را join می‌کند و همان پیام را یک بار retry می‌کند.

برای خاموش کردن این رفتار:

```bash
go run ./cmd/loadtest -phase messages -auto-setup=false
```

برای تست سقف actor روی یک ماشین، روشن بودن `auto-setup` کمک می‌کند با همان فاز messages هم actorها کم‌کم بالا بیایند.

## تست shard شده

برای چند ماشین load generator، هر ماشین یک range جدا بگیرد.

ماشین اول:

```bash
go run ./cmd/loadtest -phase all -room-start 0 -rooms 250000 -users-per-room 3 -rps 12500
```

ماشین دوم:

```bash
go run ./cmd/loadtest -phase all -room-start 250000 -rooms 250000 -users-per-room 3 -rps 12500
```

ماشین سوم:

```bash
go run ./cmd/loadtest -phase all -room-start 500000 -rooms 250000 -users-per-room 3 -rps 12500
```

ماشین چهارم:

```bash
go run ./cmd/loadtest -phase all -room-start 750000 -rooms 250000 -users-per-room 3 -rps 12500
```

جمع این چهار ماشین:

```text
1,000,000 room
50,000 RPS
```

## هشدار مهم

این loadtest فعلاً مسیر HTTP را فشار می‌دهد.

یعنی:

- RoomRegistryActor تست می‌شود
- RoomActorهای dynamic تست می‌شوند
- Redis persistence تست می‌شود
- 50k message request/s تست می‌شود

اما ۲ تا ۳ میلیون WebSocket connection واقعی را تست نمی‌کند.

برای تست WebSocket واقعی باید load generator جدا داشته باشیم که میلیون‌ها socket باز نگه دارد. آن تست به چند ماشین، افزایش file descriptor، tuning kernel، و احتمالاً چند instance از خود chat server نیاز دارد.

## دیدن وضعیت runtime روی همان ماشین

برای load test بزرگ، لاگ per-room به صورت پیش‌فرض خاموش است.
اگر برای تست کوچک خواستی روشنش کنی:

```bash
CHAT_VERBOSE_ACTORS=1 go run ./cmd/chatdemo
```

برای تست بزرگ روشنش نکن؛ terminal یا log sink خودش bottleneck می‌شود.

برای اینکه ببینی process چقدر goroutine و heap مصرف کرده:

```bash
curl localhost:8080/debug/runtime
```

برای دیدن تعداد room actorهای active:

```bash
curl localhost:8080/debug/actors
```

در این endpoint مقدار `active_count` نشان می‌دهد چند `RoomActor` در همین runtime فعال است، بدون اینکه لیست همه roomها برگردد.

برای تست سقف actor روی ماشین ۲۰ هسته‌ای، اول فقط setup را بالا ببر:

```bash
go run ./cmd/loadtest -phase setup -rooms 10000 -users-per-room 2 -workers 512
curl localhost:8080/rooms
curl localhost:8080/debug/runtime
```

بعد مقدار room را پله‌ای زیاد کن:

```text
10k
50k
100k
250k
500k
1M
```

هر پله را فقط وقتی برو بالاتر که memory و GC هنوز قابل قبول باشد.

## Bottleneck مهم در کد فعلی

در مسیر message، الان برای هر پیام این کارها انجام می‌شود:

```text
RPUSH message
LTRIM history
SET snapshot
```

برای 50k پیام در ثانیه، یعنی حداقل حدود 150k command/s سمت Redis.

این برای نمونه آموزشی خوب است، ولی برای production باید تغییر کند:

- snapshot را روی هر message ننویسیم
- snapshot دوره‌ای یا بعد از join/leave کافی است
- پیام‌ها بهتر است stream/event-log شوند
- Redis باید cluster یا shard شود
- RoomActorها باید بین چند process/server shard شوند

## پیشنهاد تست مرحله‌ای

اول local:

```bash
go run ./cmd/loadtest -phase all -rooms 1000 -users-per-room 2 -rps 500 -duration 1m
```

بعد:

```bash
go run ./cmd/loadtest -phase all -rooms 10000 -users-per-room 2 -rps 5000 -duration 2m
```

بعد روی ماشین قوی‌تر:

```bash
go run ./cmd/loadtest -phase setup -rooms 100000 -users-per-room 3 -workers 2048
go run ./cmd/loadtest -phase messages -rooms 100000 -users-per-room 3 -rps 10000 -duration 10m -workers 4096
```

بعد distributed.
