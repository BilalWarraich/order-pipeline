package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/BilalWarraich/order-pipeline/internal/worker"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// own registry instead of the global one, otherwise every test that calls
// New() would panic with "duplicate metrics collector registration"
func newRegistry(pool *worker.Pool) (*prometheus.Registry, *prometheus.HistogramVec) {
	reg := prometheus.NewRegistry()

	reqDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "http request latency",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"method", "route", "code"})

	// these just read what the pool already tracks, so the worker package
	// doesn't need to know prometheus exists
	queued := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orders_queue_depth",
		Help: "orders waiting for a worker",
	}, func() float64 { return float64(pool.Stats().Queued) })

	processed := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "orders_processed_total",
		Help: "orders that finished successfully",
	}, func() float64 { return float64(pool.Stats().Processed) })

	failed := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "orders_failed_total",
		Help: "orders that ended up failed",
	}, func() float64 { return float64(pool.Stats().Failed) })

	reg.MustRegister(
		reqDuration, queued, processed, failed,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg, reqDuration
}

func metricsMiddleware(h *prometheus.HistogramVec) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			// use the route pattern ("/orders/{id}") not the actual path, otherwise
			// every order id becomes its own time series
			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = "unmatched"
			}
			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			h.WithLabelValues(r.Method, route, strconv.Itoa(status)).Observe(time.Since(start).Seconds())
		})
	}
}
