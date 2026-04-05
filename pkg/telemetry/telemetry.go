package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	MessagesPushed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mbg_messages_pushed_total",
		Help: "The total number of messages pushed to the broker",
	})

	MessagesDispatched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mbg_messages_dispatched_total",
		Help: "The total number of messages successfully dispatched to targets",
	})

	MessagesFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mbg_messages_failed_total",
		Help: "The total number of messages that failed dispatch",
	}, []string{"reason"})

	QueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mbg_queue_size",
		Help: "Current number of messages in the queue",
	})

	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mbg_dispatcher_workers_active",
		Help: "Current number of active dispatcher workers",
	})

	DispatchLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "mbg_dispatch_latency_seconds",
		Help:    "Latency of message dispatch to targets",
		Buckets: prometheus.DefBuckets,
	})
)
