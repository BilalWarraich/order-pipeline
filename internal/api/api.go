package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/BilalWarraich/order-pipeline/internal/order"
	"github.com/BilalWarraich/order-pipeline/internal/worker"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	store   order.Store
	pool    *worker.Pool
	reg     *prometheus.Registry
	reqHist *prometheus.HistogramVec
}

func New(store order.Store, pool *worker.Pool) *Server {
	reg, hist := newRegistry(pool)
	return &Server{store: store, pool: pool, reg: reg, reqHist: hist}
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(metricsMiddleware(s.reqHist))
	r.Use(middleware.Timeout(10 * time.Second))

	r.Get("/", s.index)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	r.Get("/stats", s.stats)
	// fine for a demo, in a real deployment this shouldn't be public
	r.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))

	r.Route("/orders", func(r chi.Router) {
		r.Post("/", s.createOrder)
		r.Get("/", s.listOrders)
		r.Get("/{id}", s.getOrder)
	})
	return r
}

type createReq struct {
	CustomerID string       `json:"customer_id"`
	Items      []order.Item `json:"items"`
}

func (s *Server) createOrder(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}

	now := time.Now().UTC()
	o := &order.Order{
		ID:         newID(),
		CustomerID: req.CustomerID,
		Items:      req.Items,
		Status:     order.StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := o.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	o.TotalCts = o.Total()

	if err := s.store.Create(r.Context(), o); err != nil {
		log.Printf("create order: %v", err)
		writeErr(w, http.StatusInternalServerError, "could not save order")
		return
	}

	// order is saved as pending, so even if the queue is full nothing is lost.
	// a sweeper could pick up old pending ones later (not done yet)
	if err := s.pool.Submit(o.ID); err != nil {
		log.Printf("submit %s: %v", o.ID, err)
		w.Header().Set("Retry-After", "5")
		writeErr(w, http.StatusServiceUnavailable, "too busy, try again shortly")
		return
	}

	writeJSON(w, http.StatusAccepted, o)
}

func (s *Server) getOrder(w http.ResponseWriter, r *http.Request) {
	o, err := s.store.Get(r.Context(), chi.URLParam(r, "id"))
	if errors.Is(err, order.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		log.Printf("get order: %v", err)
		writeErr(w, http.StatusInternalServerError, "something went wrong")
		return
	}
	writeJSON(w, http.StatusOK, o)
}

func (s *Server) listOrders(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	orders, err := s.store.List(r.Context(), limit)
	if err != nil {
		log.Printf("list orders: %v", err)
		writeErr(w, http.StatusInternalServerError, "something went wrong")
		return
	}
	if orders == nil {
		orders = []*order.Order{}
	}
	writeJSON(w, http.StatusOK, orders)
}

// index is mostly so people opening the url in a browser don't just get a 404
func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "order-pipeline",
		"source":  "https://github.com/BilalWarraich/order-pipeline",
		"endpoints": []string{
			"POST /orders",
			"GET  /orders?limit=N",
			"GET  /orders/{id}",
			"GET  /stats",
			"GET  /metrics",
			"GET  /healthz",
		},
		"example": `curl -XPOST <this url>/orders -d '{"customer_id":"c1","items":[{"sku":"KB-01","qty":2,"price_cents":4999}]}'`,
	})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.pool.Stats())
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// headers are already sent at this point, nothing useful to do but log
		log.Printf("write response: %v", err)
	}
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b) // crypto/rand doesn't fail on any OS we care about
	return "ord_" + hex.EncodeToString(b)
}
