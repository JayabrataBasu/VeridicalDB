package sql

import (
	"os"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

type stubMetricsProvider struct {
	output string
}

func (s stubMetricsProvider) PrometheusMetrics() string {
	return s.output
}

func TestSessionShardMetricsProvider(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_metrics_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tm, err := catalog.NewTableManager(tmpDir, 4096, nil)
	if err != nil {
		t.Fatalf("create table manager: %v", err)
	}

	mtm := catalog.NewMVCCTableManager(tm, txn.NewManager(), nil)
	session := NewSession(mtm)
	provider := stubMetricsProvider{output: "veridicaldb_shard_queries_total 3"}

	session.SetShardMetricsProvider(provider)

	if got := session.ShardMetricsProvider(); got == nil {
		t.Fatal("expected shard metrics provider to be set")
	} else if got.PrometheusMetrics() != provider.output {
		t.Fatalf("unexpected metrics provider output: got %q want %q", got.PrometheusMetrics(), provider.output)
	}
}
