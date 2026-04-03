package storage

import (
	"mbg/config"
	"os"
	"testing"
)

func TestSQLiteTargetStorage_SaveAndGetAll(t *testing.T) {
	dbPath := "test_targets.db"
	defer os.Remove(dbPath)

	storage, err := NewSQLiteTargetStorage(dbPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// 1. Insert target without headers
	t1 := config.TargetConfig{
		Name: "TargetNoHeader",
		URL:  "http://localhost:9000/webhook1",
	}
	if err := storage.SaveTarget(t1); err != nil {
		t.Fatalf("failed to save t1: %v", err)
	}

	// 2. Insert target with headers
	t2 := config.TargetConfig{
		Name: "TargetWithHeader",
		URL:  "http://localhost:9000/webhook2",
		Headers: map[string]string{
			"Authorization": "Bearer some-token",
			"X-Custom":      "value",
		},
	}
	if err := storage.SaveTarget(t2); err != nil {
		t.Fatalf("failed to save t2: %v", err)
	}

	// 3. Verify
	targets, err := storage.GetAllTargets()
	if err != nil {
		t.Fatalf("failed to get all targets: %v", err)
	}

	if len(targets) != 2 {
		t.Errorf("expected 2 targets, got %d", len(targets))
	}

	foundT1 := false
	foundT2 := false

	for _, target := range targets {
		if target.Name == "TargetNoHeader" {
			foundT1 = true
			if target.URL != t1.URL {
				t.Errorf("expected URL %s, got %s", t1.URL, target.URL)
			}
			if len(target.Headers) != 0 {
				t.Errorf("expected 0 headers, got %d", len(target.Headers))
			}
		}
		if target.Name == "TargetWithHeader" {
			foundT2 = true
			if target.URL != t2.URL {
				t.Errorf("expected URL %s, got %s", t2.URL, target.URL)
			}
			if target.Headers["Authorization"] != "Bearer some-token" {
				t.Errorf("expected Authorization header, got %s", target.Headers["Authorization"])
			}
			if target.Headers["X-Custom"] != "value" {
				t.Errorf("expected X-Custom header, got %s", target.Headers["X-Custom"])
			}
		}
	}

	if !foundT1 {
		t.Error("TargetNoHeader not found in results")
	}
	if !foundT2 {
		t.Error("TargetWithHeader not found in results")
	}
}
