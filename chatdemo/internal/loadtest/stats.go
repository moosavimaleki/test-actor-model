package loadtest

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	startedAt int64
	ok        atomic.Uint64
	failed    atomic.Uint64
	mu        sync.Mutex
	samples   []string
}

type Snapshot struct {
	OK         uint64
	Failed     uint64
	Total      uint64
	Elapsed    time.Duration
	Throughput float64
	Samples    []string
}

// فارسی: newStats شمارنده‌های ساده و کم‌هزینه برای loadtest می‌سازد.
// فارسی: برای ۵۰k RPS، metric جمع کردن نباید خودش bottleneck جدی شود.
func newStats() *Stats {
	stats := &Stats{}
	stats.Reset()
	return stats
}

// فارسی: Reset شمارنده‌ها را برای phase بعدی صفر می‌کند.
func (s *Stats) Reset() {
	s.startedAt = time.Now().UnixNano()
	s.ok.Store(0)
	s.failed.Store(0)
	s.mu.Lock()
	s.samples = nil
	s.mu.Unlock()
}

// فارسی: Observe نتیجه یک request را ثبت می‌کند.
func (s *Stats) Observe(err error) {
	if err != nil {
		failed := s.failed.Add(1)
		if failed <= 5 {
			s.mu.Lock()
			s.samples = append(s.samples, err.Error())
			s.mu.Unlock()
		}
		return
	}

	s.ok.Add(1)
}

// فارسی: Snapshot یک کپی read-only از وضعیت فعلی stats می‌سازد.
func (s *Stats) Snapshot() Snapshot {
	ok := s.ok.Load()
	failed := s.failed.Load()
	elapsed := time.Since(time.Unix(0, atomic.LoadInt64(&s.startedAt)))
	total := ok + failed
	samples := s.copySamples()

	var throughput float64
	if elapsed > 0 {
		throughput = float64(total) / elapsed.Seconds()
	}

	return Snapshot{
		OK:         ok,
		Failed:     failed,
		Total:      total,
		Elapsed:    elapsed,
		Throughput: throughput,
		Samples:    samples,
	}
}

func (s Snapshot) String() string {
	base := fmt.Sprintf("total=%d ok=%d failed=%d elapsed=%s throughput=%.0f req/s", s.Total, s.OK, s.Failed, s.Elapsed.Round(time.Second), s.Throughput)
	if len(s.Samples) == 0 {
		return base
	}

	return base + " sample_errors=[" + strings.Join(s.Samples, " | ") + "]"
}

// فارسی: copySamples نمونه خطاها را با lock برمی‌دارد.
// فارسی: فقط چند خطای اول نگه داشته می‌شود تا خود metric گرفتن سنگین نشود.
func (s *Stats) copySamples() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, len(s.samples))
	copy(out, s.samples)
	return out
}
