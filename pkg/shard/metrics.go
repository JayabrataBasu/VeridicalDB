package shard

import (
	"fmt"
	"sync/atomic"
)

// CoordinatorMetrics tracks coordinator routing and recovery behavior.
type CoordinatorMetrics struct {
	QueriesTotal              atomic.Uint64
	SingleShardQueriesTotal   atomic.Uint64
	ScatterGatherQueriesTotal atomic.Uint64
	ReconnectAttemptsTotal    atomic.Uint64
	ReconnectSuccessesTotal   atomic.Uint64
	ReconnectFailuresTotal    atomic.Uint64
	ReadRetriesTotal          atomic.Uint64
	TimeoutErrorsTotal        atomic.Uint64
	ShardErrorsTotal          atomic.Uint64
}

// CoordinatorMetricsSnapshot is a copy of coordinator metrics values.
type CoordinatorMetricsSnapshot struct {
	QueriesTotal              uint64
	SingleShardQueriesTotal   uint64
	ScatterGatherQueriesTotal uint64
	ReconnectAttemptsTotal    uint64
	ReconnectSuccessesTotal   uint64
	ReconnectFailuresTotal    uint64
	ReadRetriesTotal          uint64
	TimeoutErrorsTotal        uint64
	ShardErrorsTotal          uint64
}

func (m *CoordinatorMetrics) snapshot() CoordinatorMetricsSnapshot {
	return CoordinatorMetricsSnapshot{
		QueriesTotal:              m.QueriesTotal.Load(),
		SingleShardQueriesTotal:   m.SingleShardQueriesTotal.Load(),
		ScatterGatherQueriesTotal: m.ScatterGatherQueriesTotal.Load(),
		ReconnectAttemptsTotal:    m.ReconnectAttemptsTotal.Load(),
		ReconnectSuccessesTotal:   m.ReconnectSuccessesTotal.Load(),
		ReconnectFailuresTotal:    m.ReconnectFailuresTotal.Load(),
		ReadRetriesTotal:          m.ReadRetriesTotal.Load(),
		TimeoutErrorsTotal:        m.TimeoutErrorsTotal.Load(),
		ShardErrorsTotal:          m.ShardErrorsTotal.Load(),
	}
}

// PrometheusMetrics returns coordinator metrics in Prometheus exposition format.
func (m *CoordinatorMetrics) PrometheusMetrics() string {
	snapshot := m.snapshot()

	return fmt.Sprintf(`# HELP veridicaldb_shard_queries_total Total number of coordinator queries
# TYPE veridicaldb_shard_queries_total counter
veridicaldb_shard_queries_total %d

# HELP veridicaldb_shard_single_shard_queries_total Total number of single-shard routed queries
# TYPE veridicaldb_shard_single_shard_queries_total counter
veridicaldb_shard_single_shard_queries_total %d

# HELP veridicaldb_shard_scatter_gather_queries_total Total number of scatter-gather routed queries
# TYPE veridicaldb_shard_scatter_gather_queries_total counter
veridicaldb_shard_scatter_gather_queries_total %d

# HELP veridicaldb_shard_reconnect_attempts_total Total number of shard reconnect attempts
# TYPE veridicaldb_shard_reconnect_attempts_total counter
veridicaldb_shard_reconnect_attempts_total %d

# HELP veridicaldb_shard_reconnect_successes_total Total number of successful shard reconnects
# TYPE veridicaldb_shard_reconnect_successes_total counter
veridicaldb_shard_reconnect_successes_total %d

# HELP veridicaldb_shard_reconnect_failures_total Total number of failed shard reconnects
# TYPE veridicaldb_shard_reconnect_failures_total counter
veridicaldb_shard_reconnect_failures_total %d

# HELP veridicaldb_shard_read_retries_total Total number of read-only retries after reconnect
# TYPE veridicaldb_shard_read_retries_total counter
veridicaldb_shard_read_retries_total %d

# HELP veridicaldb_shard_timeout_errors_total Total number of coordinator timeout errors
# TYPE veridicaldb_shard_timeout_errors_total counter
veridicaldb_shard_timeout_errors_total %d

# HELP veridicaldb_shard_errors_total Total number of shard execution errors
# TYPE veridicaldb_shard_errors_total counter
veridicaldb_shard_errors_total %d
`,
		snapshot.QueriesTotal,
		snapshot.SingleShardQueriesTotal,
		snapshot.ScatterGatherQueriesTotal,
		snapshot.ReconnectAttemptsTotal,
		snapshot.ReconnectSuccessesTotal,
		snapshot.ReconnectFailuresTotal,
		snapshot.ReadRetriesTotal,
		snapshot.TimeoutErrorsTotal,
		snapshot.ShardErrorsTotal,
	)
}
