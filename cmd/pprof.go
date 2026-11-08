package main

import (
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"time"
)

// startPprof serves net/http/pprof on FLUXMQ_PPROF_ADDR (e.g. 127.0.0.1:6060); unset = off.
func startPprof() {
	addr := os.Getenv("FLUXMQ_PPROF_ADDR")
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("pprof listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil {
			slog.Error("pprof server stopped", "error", err)
		}
	}()
}
