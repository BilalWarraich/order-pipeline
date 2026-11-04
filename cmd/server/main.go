package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/BilalWarraich/order-pipeline/internal/api"
	"github.com/BilalWarraich/order-pipeline/internal/order"
	"github.com/BilalWarraich/order-pipeline/internal/sweeper"
	"github.com/BilalWarraich/order-pipeline/internal/worker"
	"github.com/BilalWarraich/order-pipeline/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	addr := env("ADDR", ":8080")
	// most hosting platforms (render, koyeb, cloud run...) pass the port this way
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}
	workers := envInt("WORKERS", 4)
	queueSize := envInt("QUEUE_SIZE", 100)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var store order.Store
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		db, err := pgxpool.New(ctx, dsn)
		if err != nil {
			log.Fatalf("db: %v", err)
		}
		defer db.Close()
		if err := db.Ping(ctx); err != nil {
			log.Fatalf("db ping: %v", err)
		}
		if err := migrations.Run(ctx, db); err != nil {
			log.Fatalf("migrations: %v", err)
		}
		store = order.NewPGStore(db)
		log.Println("using postgres")
	} else {
		store = order.NewMemStore()
		log.Println("DATABASE_URL not set, using in-memory store")
	}

	failRate, _ := strconv.ParseFloat(os.Getenv("FAIL_RATE"), 64)
	proc := &order.Processor{Store: store, FailRate: failRate, Timeout: 5 * time.Second}
	pool := worker.NewPool(workers, queueSize, proc.Process)

	stuckProcessing := time.Duration(envInt("STUCK_PROCESSING_SECS", 300)) * time.Second
	if stuckProcessing > 0 && stuckProcessing < 2*proc.Timeout {
		// a live worker could still be on an order this young
		log.Printf("warning: STUCK_PROCESSING_SECS is less than 2x the processor timeout (%v), orders might get processed twice", proc.Timeout)
	}
	sw := &sweeper.Sweeper{
		Store:           store,
		Submit:          pool.Submit,
		Interval:        30 * time.Second,
		MaxAge:          time.Duration(envInt("STUCK_AFTER_SECS", 120)) * time.Second,
		StuckProcessing: stuckProcessing,
	}
	sweeperDone := make(chan struct{})
	go func() {
		sw.Run(ctx)
		close(sweeperDone)
	}()

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(store, pool).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("listening on %s (%d workers, queue %d)", addr, workers, queueSize)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// stop taking new http requests first, then let the workers finish what's queued
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	// ctx is already cancelled so the sweeper is on its way out, wait for it
	// so it doesn't submit into a closed pool
	<-sweeperDone
	if err := pool.Shutdown(shutdownCtx); err != nil {
		log.Printf("worker shutdown: %v (some orders may still be pending)", err)
	}
	log.Println("bye")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v, err := strconv.Atoi(os.Getenv(k))
	if err != nil {
		return def
	}
	return v
}
