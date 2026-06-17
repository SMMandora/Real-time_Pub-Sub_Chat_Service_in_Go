package gateway

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// ActiveConnections is the number of currently connected WebSocket clients.
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chat_active_connections",
		Help: "Currently connected WebSocket clients.",
	})
	// MessagesTotal counts chat messages published to the bus.
	MessagesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "chat_messages_total",
		Help: "Chat messages published to the bus.",
	})
	// FanoutLatencySeconds measures send-to-local-delivery latency.
	FanoutLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "chat_fanout_latency_seconds",
		Help:    "Latency from message send to local delivery.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	})
)

// metricsHandler serves the default Prometheus registry.
func metricsHandler() http.Handler { return promhttp.Handler() }
