package loadtest

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// فارسی: runMessages فشار اصلی message را تولید می‌کند.
// فارسی: برای ۱ میلیون room و هر room هر ۲۰ ثانیه یک پیام، RPS هدف برابر ۵۰ هزار است.
func runMessages(cfg Config, client *Client, stats *Stats) error {
	log.Printf("messages started rooms=%d rps=%d duration=%s workers=%d", cfg.Rooms, cfg.RPS, cfg.Duration, cfg.Workers)
	if err := preflightMessages(cfg, client); err != nil {
		if !cfg.AutoSetup {
			return err
		}
		log.Printf("message preflight warning: %v", err)
		log.Printf("auto-setup is enabled; missing rooms will be created on demand during messages")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	jobs := make(chan Job, cfg.Workers*8)
	done := make(chan struct{})

	go reportLoop(ctx, "messages", cfg.ReportEvery, stats, done)
	go produceMessageJobs(ctx, cfg, client, jobs)

	// فارسی: context زمان‌دار فقط تولید job را متوقف می‌کند.
	// فارسی: خود requestهای صف‌شده با timeout داخلی HTTP client کنترل می‌شوند تا پایان تست خطای مصنوعی نسازد.
	runWorkers(context.Background(), cfg.Workers, jobs, stats)
	close(done)
	log.Printf("messages done %s", stats.Snapshot().String())
	return nil
}

// فارسی: produceMessageJobs با batchهای ۱۰۰ میلی‌ثانیه‌ای request تولید می‌کند.
// فارسی: این روش برای RPS بالا از tickerهای خیلی ریز و پرهزینه بهتر است.
func produceMessageJobs(ctx context.Context, cfg Config, client *Client, jobs chan<- Job) {
	defer close(jobs)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	perTick := cfg.RPS / 10
	if perTick <= 0 {
		perTick = 1
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i := 0; i < perTick; i++ {
				roomNumber := cfg.RoomStart + rnd.Intn(cfg.Rooms)
				userIndex := rnd.Intn(cfg.UsersPerRoom)
				seq := time.Now().UnixNano()
				if !submitMessageJob(ctx, jobs, messageJob(cfg, client, roomNumber, userIndex, seq)) {
					return
				}
			}
		}
	}
}

// فارسی: submitMessageJob موقع پر شدن صف، به cancellation تست احترام می‌گذارد.
// فارسی: بدون این select، producer ممکن است روی ارسال به jobs گیر کند.
func submitMessageJob(ctx context.Context, jobs chan<- Job, job Job) bool {
	select {
	case <-ctx.Done():
		return false
	case jobs <- job:
		return true
	}
}

func messageJob(cfg Config, client *Client, roomNumber, userIndex int, seq int64) Job {
	return func(ctx context.Context) error {
		body := map[string]string{
			"from": nickForUser(roomNumber, userIndex),
			"text": fmt.Sprintf("load-test-message-%d", seq),
		}
		err := client.postJSON(ctx, "/rooms/"+roomID(roomNumber)+"/message", body)
		if err == nil || !cfg.AutoSetup || !isMissingSetupError(err) {
			return err
		}

		if setupErr := ensureRoomSetup(ctx, cfg, client, roomNumber); setupErr != nil {
			return setupErr
		}

		return client.postJSON(ctx, "/rooms/"+roomID(roomNumber)+"/message", body)
	}
}
