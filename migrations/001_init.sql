-- migrations/001_init.sql
-- Minimal schema for MVP task engine

BEGIN;

-- Task states we will use in MVP:
-- PENDING, ENQUEUED, RUNNING, SUCCEEDED, FAILED, RETRY_WAIT

CREATE TABLE IF NOT EXISTS tasks (
  id              UUID PRIMARY KEY,
  state           TEXT NOT NULL,
  image           TEXT NOT NULL,
  cmd             JSONB NOT NULL DEFAULT '[]'::jsonb,
  env             JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  next_run_at     TIMESTAMPTZ NULL,

  attempt         INT NOT NULL DEFAULT 0,
  max_attempts    INT NOT NULL DEFAULT 3,

  last_error      TEXT NULL,
  exit_code       INT NULL,
  logs            TEXT NULL
);

-- Ownership / lease + fencing token
CREATE TABLE IF NOT EXISTS task_ownership (
  task_id           UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
  owner_id          TEXT NOT NULL,
  lease_expires_at  TIMESTAMPTZ NOT NULL,
  last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  fencing_token     BIGINT NOT NULL
);

-- Useful indexes for schedulers and lookups
CREATE INDEX IF NOT EXISTS idx_tasks_state_runat
  ON tasks(state, run_at);

CREATE INDEX IF NOT EXISTS idx_tasks_state_nextrunat
  ON tasks(state, next_run_at);

CREATE INDEX IF NOT EXISTS idx_ownership_lease_exp
  ON task_ownership(lease_expires_at);

COMMIT;
