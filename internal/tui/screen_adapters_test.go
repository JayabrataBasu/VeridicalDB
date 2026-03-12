package tui

import (
	"os"
	"strings"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

type stubMetricsProvider struct {
	output string
}

func (s stubMetricsProvider) PrometheusMetrics() string {
	return s.output
}

func TestBuildSystemCatalogIncludesShardMetricsProvider(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tui_system_catalog_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tm, err := catalog.NewTableManager(tmpDir, 4096, nil)
	if err != nil {
		t.Fatalf("create table manager: %v", err)
	}

	mtm := catalog.NewMVCCTableManager(tm, txn.NewManager(), nil)
	session := sql.NewSession(mtm)
	session.SetShardMetricsProvider(stubMetricsProvider{output: "veridicaldb_shard_queries_total 7"})

	app := New(session)
	sc := buildSystemCatalog(app)
	if sc == nil {
		t.Fatal("expected system catalog")
	}

	metrics := sc.PrometheusMetrics()
	if !strings.Contains(metrics, "veridicaldb_shard_queries_total 7") {
		t.Fatalf("expected shard metrics in system catalog output, got %q", metrics)
	}
}
