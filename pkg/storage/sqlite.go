package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"mbg/config"

	_ "modernc.org/sqlite"
)

type TargetStorage interface {
	SaveTarget(target config.TargetConfig) error
	GetAllTargets() ([]config.TargetConfig, error)
	Close() error
}

type SQLiteTargetStorage struct {
	db *sql.DB
}

func NewSQLiteTargetStorage(dbPath string) (*SQLiteTargetStorage, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite: %w", err)
	}

	s := &SQLiteTargetStorage{db: db}
	if err := s.Init(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *SQLiteTargetStorage) Init() error {
	// Enable WAL mode for better concurrency
	if _, err := s.db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	query := `
	CREATE TABLE IF NOT EXISTS targets (
		name TEXT PRIMARY KEY,
		url TEXT NOT NULL,
		headers TEXT
	);`
	_, err := s.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create targets table: %w", err)
	}
	return nil
}

func (s *SQLiteTargetStorage) SaveTarget(target config.TargetConfig) error {
	headersJSON, err := json.Marshal(target.Headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %w", err)
	}

	query := `INSERT INTO targets (name, url, headers) VALUES (?, ?, ?) 
	          ON CONFLICT(name) DO UPDATE SET url=excluded.url, headers=excluded.headers`
	_, err = s.db.Exec(query, target.Name, target.URL, string(headersJSON))
	if err != nil {
		return fmt.Errorf("failed to save target: %w", err)
	}
	return nil
}

func (s *SQLiteTargetStorage) GetAllTargets() ([]config.TargetConfig, error) {
	query := `SELECT name, url, headers FROM targets`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query targets: %w", err)
	}
	defer rows.Close()

	var targets []config.TargetConfig
	for rows.Next() {
		var t config.TargetConfig
		var headersStr sql.NullString
		if err := rows.Scan(&t.Name, &t.URL, &headersStr); err != nil {
			return nil, fmt.Errorf("failed to scan target: %w", err)
		}

		if headersStr.Valid && headersStr.String != "" {
			if err := json.Unmarshal([]byte(headersStr.String), &t.Headers); err != nil {
				return nil, fmt.Errorf("failed to unmarshal headers for %s: %w", t.Name, err)
			}
		} else {
			t.Headers = make(map[string]string)
		}
		targets = append(targets, t)
	}
	return targets, nil
}

func (s *SQLiteTargetStorage) Close() error {
	return s.db.Close()
}
