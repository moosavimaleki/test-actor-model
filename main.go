package main

import (
	"fmt"
	"log"
	"time"

	ergo "ergo.services/ergo"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

// Inc پیام افزایش counter است.
//
// این struct هم برای Send استفاده می‌شود، هم برای Call.
// یعنی هم می‌توانی بگویی:
//
//	فقط زیادش کن، جواب نمی‌خواهم.
//
// و هم می‌توانی بگویی:
//
//	زیادش کن و مقدار جدید را به من برگردان.
type Inc struct {
	By int
}

// Get پیام گرفتن مقدار فعلی counter است.
//
// این پیام معمولاً با Call فرستاده می‌شود، چون caller جواب می‌خواهد.
type Get struct{}

// Stop پیام توقف actor است.
//
// به‌جای اینکه string خام مثل "stop" بفرستیم، type مشخص می‌سازیم
// تا کد type-safeتر و خواناتر شود.
type Stop struct{}

// CounterActor همان actor ماست.
//
// act.Actor را embed کرده‌ایم تا Ergo بتواند این struct را به عنوان actor اجرا کند.
//
// value state داخلی actor است.
// نکته مهم:
// این value را هیچ goroutine دیگری مستقیم تغییر نمی‌دهد.
// فقط خود actor در HandleMessage یا HandleCall آن را تغییر می‌دهد.
// چون actor پیام‌ها را sequential پردازش می‌کند، اینجا mutex لازم نداریم.
type CounterActor struct {
	act.Actor

	value int
}

// newCounterActor factory function است.
//
// Ergo برای ساخت process/actor از factory استفاده می‌کند.
// هر بار که Spawn صدا زده شود، این تابع یک instance تازه از CounterActor می‌سازد.
//
// خروجی gen.ProcessBehavior است، چون act.Actor در نهایت abstraction راحت‌تری
// روی همان ProcessBehavior سطح پایین Ergo است.
func newCounterActor() gen.ProcessBehavior {
	return &CounterActor{}
}

// Init فقط یک بار، هنگام ساخته شدن actor اجرا می‌شود.
//
// اینجا جای initialize کردن state است.
// مثلاً connection، config، مقدار اولیه، child actorها و غیره.
//
// اگر این تابع error برگرداند، actor اصلاً start نمی‌شود.
func (c *CounterActor) Init(args ...any) error {
	// مقدار اولیه state داخلی actor
	c.value = 0

	fmt.Println("counter actor started. pid =", c.PID())

	// nil یعنی initialization موفق بود و actor می‌تواند وارد حالت running شود.
	return nil
}

// HandleMessage برای پیام‌های async است.
//
// یعنی پیام‌هایی که با Send فرستاده می‌شوند.
// caller منتظر جواب نمی‌ماند.
// پیام وارد mailbox actor می‌شود و actor هر وقت نوبتش شد آن را پردازش می‌کند.
//
// مثال:
//
//	node.Send(pid, Inc{By: 1})
//
// اینجا جواب مستقیم به caller برنمی‌گردانیم.
func (c *CounterActor) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {

	case Inc:
		// چون این state فقط داخل actor تغییر می‌کند،
		// نیازی به mutex نداریم.
		c.value += msg.By

		fmt.Printf("async increment by %d => value=%d\n", msg.By, c.value)

		// nil یعنی actor بعد از پردازش این پیام زنده بماند.
		return nil

	case Stop:
		fmt.Println("stop message received")

		// این error خاص یعنی actor تمیز و عادی terminate شود.
		return gen.TerminateReasonNormal

	default:
		// پیام ناشناخته را ignore می‌کنیم.
		// در production بهتر است log شود.
		fmt.Printf("unknown async message: %T\n", message)

		return nil
	}
}

// HandleCall برای پیام‌های sync است.
//
// یعنی پیام‌هایی که با Call فرستاده می‌شوند.
// caller منتظر response می‌ماند.
//
// مثال:
//
//	result, err := node.Call(pid, Get{})
//
// نکته خیلی مهم در Ergo:
// خروجی اول result است و به caller برمی‌گردد.
// خروجی دوم error برای terminate کردن actor است، نه application error.
//
// یعنی اگر می‌خواهی به caller بگویی درخواست اشتباه بود، بهتر است error را
// به عنوان result برگردانی و error دوم را nil نگه داری.
func (c *CounterActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	switch req := request.(type) {

	case Get:
		// caller مقدار فعلی counter را خواسته.
		// result = c.value
		// error = nil یعنی actor زنده بماند.
		return c.value, nil

	case Inc:
		// اینجا Call همزمان state را تغییر می‌دهد و جواب هم می‌دهد.
		// یعنی caller بعد از افزایش، مقدار جدید را می‌گیرد.
		c.value += req.By

		return c.value, nil

	default:
		// این application-level error است.
		// یعنی actor نباید بمیرد.
		// پس error را در result برمی‌گردانیم، نه در خروجی دوم.
		return fmt.Errorf("unknown sync request: %T", request), nil
	}
}

// Terminate هنگام پایان actor اجرا می‌شود.
//
// اینجا جای cleanup است:
// بستن connection، flush کردن buffer، log نهایی و غیره.
//
// این تابع بعد از terminate شدن actor صدا زده می‌شود.
func (c *CounterActor) Terminate(reason error) {
	fmt.Println("counter actor terminated. reason =", reason)
}

// main نقطه شروع برنامه است.
//
// اینجا:
// 1. یک Ergo Node می‌سازیم.
// 2. actor را Spawn می‌کنیم.
// 3. چند پیام async با Send می‌فرستیم.
// 4. چند request sync با Call می‌فرستیم.
// 5. در آخر actor را stop می‌کنیم.
func main() {
	// Node یعنی runtime اصلی Ergo.
	// Actorها داخل node زندگی می‌کنند.
	// node مسئول process lifecycle، message routing و ارتباط بین actorهاست.
	node, err := ergo.StartNode("demo@localhost", gen.NodeOptions{})
	if err != nil {
		log.Fatal(err)
	}

	// در پایان برنامه node را stop می‌کنیم.
	// در برنامه واقعی معمولاً graceful shutdown جدی‌تر طراحی می‌کنی.
	defer node.Stop()

	// Spawn یعنی ساختن یک actor/process جدید داخل node.
	//
	// pid آدرس actor است.
	// از این به بعد برای حرف زدن با actor از همین pid استفاده می‌کنیم.
	pid, err := node.Spawn(newCounterActor, gen.ProcessOptions{})
	if err != nil {
		log.Fatal(err)
	}

	// Send یعنی پیام async.
	// caller منتظر جواب نمی‌ماند.
	// پیام فقط وارد mailbox actor می‌شود.
	_ = node.Send(pid, Inc{By: 1})
	_ = node.Send(pid, Inc{By: 2})

	// فقط برای اینکه در demo فرصت بدهیم پیام‌های async چاپ شوند.
	// در کد واقعی معمولاً با sleep طراحی نمی‌کنی.
	time.Sleep(100 * time.Millisecond)

	// Call یعنی request/response.
	// caller منتظر جواب actor می‌ماند.
	result, err := node.Call(pid, Get{})

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("sync get result =", result)

	// این Call هم state را تغییر می‌دهد، هم مقدار جدید را برمی‌گرداند.
	result, err = node.Call(pid, Inc{By: 10})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("sync increment result =", result)

	// توقف actor با پیام async.
	_ = node.Send(pid, Stop{})

	time.Sleep(100 * time.Millisecond)
}
