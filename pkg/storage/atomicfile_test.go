package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.json")

	if err := WriteFileAtomic(path, []byte(`{"v":1}`), 0o644); err != nil {
		t.Fatalf("first write: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != `{"v":1}` {
		t.Fatalf("read back = %q, %v", got, err)
	}

	// Overwrite; the old content must never be observable as truncated.
	if err := WriteFileAtomic(path, []byte(`{"v":22222}`), 0o644); err != nil {
		t.Fatalf("second write: %v", err)
	}
	got, _ = os.ReadFile(path)
	if string(got) != `{"v":22222}` {
		t.Fatalf("after overwrite = %q", got)
	}

	// No temp files left behind.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("expected only catalog.json, got %v", names)
	}

	if fi, _ := os.Stat(path); fi != nil && fi.Mode().Perm() != 0o644 {
		t.Errorf("perm = %v, want 0644", fi.Mode().Perm())
	}
}

func TestWriteFileAtomicRejectsBadDir(t *testing.T) {
	if err := WriteFileAtomic(filepath.Join(t.TempDir(), "nope", "x.json"), []byte("x"), 0o644); err == nil {
		t.Fatal("expected an error writing into a nonexistent directory")
	}
}
