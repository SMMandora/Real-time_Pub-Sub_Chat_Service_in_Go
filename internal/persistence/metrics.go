package persistence

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// MessagesPersisted counts messages written to Postgres.
	MessagesPersisted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "chat_messages_persisted_total",
		Help: "Messages written to Postgres.",
	})
	// BatchSize observes the number of rows per persistence batch.
	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "chat_persist_batch_size",
		Help:    "Rows per persistence batch.",
		Buckets: []float64{1, 5, 10, 25, 50, 100},
	})
	// QueueDepth is the number of messages waiting in the batcher input channel.
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chat_persist_queue_depth",
		Help: "Pending messages in the batcher input channel.",
	})
)
