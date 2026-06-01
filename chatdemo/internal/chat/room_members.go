package chat

import (
	"sort"
	"strings"

	"ergo.services/ergo/gen"
)

// فارسی: join کاربر را وارد room می‌کند و snapshot جدید را persist می‌کند.
// فارسی: اگر conn خالی باشد، یعنی join از HTTP آمده و broadcast زنده ندارد.
func (r *RoomActor) join(nick string, conn gen.PID) Result {
	nick = strings.TrimSpace(nick)
	if nick == "" {
		return Fail("nick is empty")
	}

	// فارسی: با assignment روی map، join دوباره همان nick اتصال قبلی را replace می‌کند.
	// فارسی: این برای reconnect ساده است، ولی برای auth واقعی باید policy جدا داشته باشی.
	r.members[nick] = roomMember{nick: nick, pid: conn}
	if err := r.persistSnapshot(); err != nil {
		return Fail("snapshot failed: " + err.Error())
	}

	// فارسی: بعد از موفق شدن persistence، event به connectionهای زنده broadcast می‌شود.
	r.broadcast(RoomEvent{
		Type:    "join",
		RoomID:  r.roomID,
		From:    nick,
		Text:    nick + " joined",
		Members: r.memberList(),
	})
	return OK()
}

// فارسی: leave کاربر را از room حذف می‌کند.
// فارسی: اگر conn داده شده باشد، فقط همان connection حق حذف عضویت خودش را دارد.
func (r *RoomActor) leave(nick string, conn gen.PID) Result {
	nick = strings.TrimSpace(nick)
	if nick == "" {
		return Fail("nick is empty")
	}

	member, exists := r.members[nick]
	if !exists {
		return OK()
	}

	// فارسی: اگر socket قدیمی بعد از reconnect بسته شود،
	// فارسی: نباید عضویت socket جدید را پاک کند.
	if conn != (gen.PID{}) && member.pid != conn {
		return OK()
	}

	delete(r.members, nick)
	if err := r.persistSnapshot(); err != nil {
		return Fail("snapshot failed: " + err.Error())
	}

	r.broadcast(RoomEvent{
		Type:    "leave",
		RoomID:  r.roomID,
		From:    nick,
		Text:    nick + " left",
		Members: r.memberList(),
	})
	return OK()
}

// فارسی: memberList خروجی deterministic می‌سازد.
// فارسی: sort برای تست و خوانایی API مهم است، وگرنه map در Go ترتیب ثابت ندارد.
func (r *RoomActor) memberList() []string {
	members := make([]string, 0, len(r.members))
	for nick := range r.members {
		members = append(members, nick)
	}

	sort.Strings(members)
	return members
}

// فارسی: broadcast event را به همه connection actorهای زنده می‌فرستد.
// فارسی: Send async است؛ RoomActor منتظر جواب socketها نمی‌ماند.
func (r *RoomActor) broadcast(event RoomEvent) {
	for _, member := range r.members {
		// فارسی: عضوی که از HTTP join کرده باشد PID ندارد، پس push زنده دریافت نمی‌کند.
		if member.pid == (gen.PID{}) {
			continue
		}

		_ = r.Send(member.pid, event)
	}
}
