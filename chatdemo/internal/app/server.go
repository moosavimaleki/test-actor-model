package app

import (
	"errors"
	"log"
	"net/http"
	"time"

	"ergo.services/ergo/gen"

	"actor-chat-demo/internal/httpapi"
)

// فارسی: newHTTPServer تنظیمات عملیاتی HTTP server را متمرکز می‌کند.
// فارسی: timeoutها مهم‌اند چون request کند نباید goroutine/server را بی‌نهایت نگه دارد.
func newHTTPServer(node gen.Node, registryPID gen.PID) *http.Server {
	return &http.Server{
		Addr:              ":8080",
		Handler:           httpapi.New(node, registryPID),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// فارسی: startHTTPServer سرور را در goroutine جدا اجرا می‌کند.
// فارسی: main goroutine باید آزاد بماند تا بتواند منتظر signal shutdown شود.
func startHTTPServer(server *http.Server) <-chan error {
	serverErr := make(chan error, 1)
	go func() {
		log.Println("HTTP API listening on :8080")

		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}

		serverErr <- nil
	}()
	return serverErr
}
