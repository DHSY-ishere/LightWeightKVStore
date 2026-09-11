// Command kvstore starts a Redis-protocol-compatible, in-memory
// key-value server on port 6379.
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kvstore/server"
	"kvstore/store"
)

const sweepInterval = 100 * time.Millisecond

func main() {
	db := store.New()
	srv := server.New(":6379", db)

	// Active expiration: without this, a key set with EX that is never
	// GET'd again would sit in memory forever, since lazy deletion in
	// store.Get only reclaims a key when something actually reads it.
	// The sweeper walks every shard on a fixed interval and evicts
	// anything past its TTL; store.Sweep locks/scans/unlocks one shard
	// at a time so it never holds up more than one shard's traffic at
	// once.
	sweepDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(sweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				db.Sweep()
			case <-sweepDone:
				return
			}
		}
	}()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		close(sweepDone)
		if err != nil {
			log.Fatalf("server error: %v", err)
		}

	case sig := <-sigCh:
		log.Printf("received %s, shutting down gracefully...", sig)

		// Order matters: stop generating new work (sweeper) and stop
		// accepting new clients/close existing ones (server) before
		// the process exits. The store itself needs no shutdown step —
		// it's purely in-memory with nothing to flush to disk.
		close(sweepDone)
		srv.Shutdown()

		log.Println("shutdown complete")
	}
}
