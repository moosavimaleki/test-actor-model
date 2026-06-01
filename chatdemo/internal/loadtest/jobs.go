package loadtest

import (
	"context"
	"sync"
)

type Job func(context.Context) error

// فارسی: runWorkers یک worker pool ساده برای اجرای requestهاست.
// فارسی: producer job تولید می‌کند و workerها همزمان requestها را می‌فرستند.
func runWorkers(ctx context.Context, workers int, jobs <-chan Job, stats *Stats) {
	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()
			for job := range jobs {
				stats.Observe(job(ctx))
			}
		}()
	}

	wg.Wait()
}
