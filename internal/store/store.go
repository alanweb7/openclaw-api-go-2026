package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
)

type SessionDelivery struct {
	CallbackURL string
	Stream      bool
	UpdatedAt   time.Time
}

type Store struct {
	db      *sql.DB
	enabled bool
	logger  *slog.Logger
}

func New(ctx context.Context, cfg config.Config, logger *slog.Logger) *Store {
	s := &Store{
		enabled: false,
		logger:  logger.With("component", "store"),
	}
	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		s.logger.Info("database disabled: DATABASE_URL not set")
		return s
	}

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		s.logger.Error("database open failed, continuing without persistence", "error", err.Error())
		return s
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		s.logger.Error("database ping failed, continuing without persistence", "error", err.Error())
		_ = db.Close()
		return s
	}

	s.db = db
	s.enabled = true
	if err := s.ensureSchema(ctx); err != nil {
		s.logger.Error("database schema init failed, continuing without persistence", "error", err.Error())
		_ = db.Close()
		s.db = nil
		s.enabled = false
		return s
	}
	s.logger.Info("database persistence enabled")
	return s
}

func (s *Store) Enabled() bool { return s.enabled && s.db != nil }

func (s *Store) ensureSchema(ctx context.Context) error {
	if !s.Enabled() {
		return nil
	}
	ddl := []string{
		`create table if not exists session_delivery (
			session_key text primary key,
			callback_url text not null default '',
			stream boolean not null default true,
			updated_at timestamptz not null default now()
		);`,
		`create table if not exists dedupe_keys (
			key text primary key,
			kind text not null,
			expires_at timestamptz not null,
			created_at timestamptz not null default now()
		);`,
		`create index if not exists idx_dedupe_expires_at on dedupe_keys (expires_at);`,
	}
	for _, q := range ddl {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) UpsertSessionDelivery(ctx context.Context, sessionKey string, opts SessionDelivery) error {
	if !s.Enabled() {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		insert into session_delivery (session_key, callback_url, stream, updated_at)
		values ($1, $2, $3, now())
		on conflict (session_key) do update
		set callback_url = excluded.callback_url,
			stream = excluded.stream,
			updated_at = now()
	`, sessionKey, opts.CallbackURL, opts.Stream)
	return err
}

func (s *Store) GetSessionDelivery(ctx context.Context, sessionKey string) (SessionDelivery, bool, error) {
	if !s.Enabled() {
		return SessionDelivery{}, false, nil
	}
	var cb string
	var stream bool
	var updated time.Time
	err := s.db.QueryRowContext(ctx, `
		select callback_url, stream, updated_at
		from session_delivery
		where session_key = $1
	`, sessionKey).Scan(&cb, &stream, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionDelivery{}, false, nil
	}
	if err != nil {
		return SessionDelivery{}, false, err
	}
	return SessionDelivery{
		CallbackURL: cb,
		Stream:      stream,
		UpdatedAt:   updated,
	}, true, nil
}

func (s *Store) DeleteSessionDelivery(ctx context.Context, sessionKey string) error {
	if !s.Enabled() {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `delete from session_delivery where session_key = $1`, sessionKey)
	return err
}

func (s *Store) RegisterDedupeKey(ctx context.Context, kind, key string, ttl time.Duration) (bool, error) {
	if !s.Enabled() {
		return true, nil
	}
	if strings.TrimSpace(key) == "" {
		return true, nil
	}
	exp := time.Now().UTC().Add(ttl)
	res, err := s.db.ExecContext(ctx, `
		insert into dedupe_keys (key, kind, expires_at)
		values ($1, $2, $3)
		on conflict do nothing
	`, key, kind, exp)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) CleanupExpiredDedupeKeys(ctx context.Context) error {
	if !s.Enabled() {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `delete from dedupe_keys where expires_at < now()`)
	return err
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) DebugInfo() string {
	if !s.Enabled() {
		return "disabled"
	}
	return fmt.Sprintf("enabled")
}
