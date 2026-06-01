package loadtest

import (
	"context"
	"log"
)

// فارسی: runSetup roomها را می‌سازد و برای هر room چند member join می‌کند.
// فارسی: اگر ۱ میلیون room و ۳ user داشته باشی، setup حدود ۴ میلیون request می‌زند.
func runSetup(cfg Config, client *Client, stats *Stats) error {
	log.Printf("setup started rooms=%d users_per_room=%d workers=%d", cfg.Rooms, cfg.UsersPerRoom, cfg.Workers)

	ctx := context.Background()
	runSetupStage(ctx, "setup-create", cfg, stats, func(jobs chan<- Job) {
		for roomNumber := cfg.RoomStart; roomNumber < cfg.RoomStart+cfg.Rooms; roomNumber++ {
			submitCreateRoom(jobs, client, roomNumber)
		}
	})

	runSetupStage(ctx, "setup-join", cfg, stats, func(jobs chan<- Job) {
		for roomNumber := cfg.RoomStart; roomNumber < cfg.RoomStart+cfg.Rooms; roomNumber++ {
			for userIndex := 0; userIndex < cfg.UsersPerRoom; userIndex++ {
				submitJoinRoom(jobs, client, roomNumber, userIndex)
			}
		}
	})

	log.Printf("setup done %s", stats.Snapshot().String())
	return nil
}

// فارسی: runSetupStage ترتیب setup را قابل کنترل می‌کند.
// فارسی: اول همه roomها ساخته می‌شوند، بعد joinها شروع می‌شوند تا race با room not found نداشته باشیم.
func runSetupStage(ctx context.Context, phase string, cfg Config, stats *Stats, produce func(chan<- Job)) {
	jobs := make(chan Job, cfg.Workers*2)
	done := make(chan struct{})

	go reportLoop(ctx, phase, cfg.ReportEvery, stats, done)
	go func() {
		defer close(jobs)
		produce(jobs)
	}()

	runWorkers(ctx, cfg.Workers, jobs, stats)
	close(done)
	log.Printf("%s done %s", phase, stats.Snapshot().String())
}

func submitCreateRoom(jobs chan<- Job, client *Client, roomNumber int) {
	jobs <- func(ctx context.Context) error {
		return createRoom(ctx, client, roomNumber)
	}
}

func submitJoinRoom(jobs chan<- Job, client *Client, roomNumber, userIndex int) {
	jobs <- func(ctx context.Context) error {
		return joinRoom(ctx, client, roomNumber, userIndex)
	}
}
