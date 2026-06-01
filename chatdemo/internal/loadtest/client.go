package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e HTTPError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("unexpected status %d", e.StatusCode)
	}

	return fmt.Sprintf("unexpected status %d body=%s", e.StatusCode, e.Body)
}

type Client struct {
	baseURL string
	http    *http.Client
}

// فارسی: newClient یک HTTP client با connection pool بزرگ می‌سازد.
// فارسی: برای RPS بالا، ساخت connection جدید برای هر request تست را خراب می‌کند.
func newClient(cfg Config) *Client {
	transport := &http.Transport{
		MaxIdleConns:        cfg.Workers * 4,
		MaxIdleConnsPerHost: cfg.Workers * 4,
		MaxConnsPerHost:     cfg.Workers * 4,
		IdleConnTimeout:     90 * time.Second,
	}

	return &Client{
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		http: &http.Client{
			Transport: transport,
			Timeout:   cfg.Timeout,
		},
	}
}

// فارسی: postJSON یک درخواست POST JSON می‌فرستد.
// فارسی: statusهای 2xx موفق حساب می‌شوند؛ بقیه به عنوان خطای loadtest ثبت می‌شوند.
func (c *Client) postJSON(ctx context.Context, path string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	return c.do(req)
}

// فارسی: getJSON یک درخواست GET می‌زند و response JSON را داخل dst decode می‌کند.
// فارسی: این برای preflight استفاده می‌شود تا قبل از فشار اصلی بفهمیم setup انجام شده یا نه.
func (c *Client) getJSON(ctx context.Context, path string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}

	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(io.LimitReader(res.Body, 4096))
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return HTTPError{StatusCode: res.StatusCode, Body: string(body)}
	}

	return json.Unmarshal(body, dst)
}

// فارسی: do اجرای مشترک requestهاست.
// فارسی: body همیشه drain و close می‌شود تا connection دوباره قابل استفاده باشد.
func (c *Client) do(req *http.Request) error {
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
	_, _ = io.Copy(io.Discard, res.Body)
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return HTTPError{StatusCode: res.StatusCode, Body: string(body)}
	}

	return nil
}
