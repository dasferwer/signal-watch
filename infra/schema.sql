CREATE TABLE catalog_state (
    id integer PRIMARY KEY CHECK (id=1),
    sequence bigint NOT NULL DEFAULT 0
);
INSERT INTO catalog_state(id) VALUES(1);
CREATE TABLE events (
    sequence bigint PRIMARY KEY,
    event_id uuid UNIQUE NOT NULL,
    occurred_at double precision NOT NULL,
    body_hash text NOT NULL,
    body jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_time ON events(occurred_at);
CREATE TABLE runs (
    id uuid PRIMARY KEY,
    kind text NOT NULL CHECK(kind IN ('live','replay')),
    status text NOT NULL CHECK(status IN ('running','completed')) DEFAULT 'running',
    cursor bigint NOT NULL DEFAULT 0,
    upper_bound bigint,
    watermark double precision NOT NULL DEFAULT 0,
    model_version text,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);
CREATE UNIQUE INDEX single_live_run ON runs(kind) WHERE kind='live';
INSERT INTO runs(id,kind) VALUES('00000000-0000-0000-0000-000000000023','live');
CREATE TABLE decisions (
    run_id uuid NOT NULL REFERENCES runs(id),
    event_sequence bigint NOT NULL REFERENCES events(sequence),
    status text NOT NULL CHECK(status IN ('scored','late')),
    features jsonb NOT NULL,
    result jsonb NOT NULL,
    alert boolean NOT NULL,
    processing_ms double precision NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(run_id,event_sequence)
);
CREATE INDEX decisions_alerts ON decisions(run_id,event_sequence) WHERE alert;
CREATE TABLE outbox (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES runs(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX pending_outbox ON outbox(id) WHERE published_at IS NULL;
CREATE TABLE reviews (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL,
    event_sequence bigint NOT NULL,
    version integer NOT NULL,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY(run_id,event_sequence) REFERENCES decisions(run_id,event_sequence),
    UNIQUE(run_id,event_sequence,version)
);
CREATE TABLE heartbeats (name text PRIMARY KEY, seen_at timestamptz NOT NULL DEFAULT now());
CREATE FUNCTION reject_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'This journal is append-only';
END $$;
CREATE TRIGGER immutable_events BEFORE UPDATE OR DELETE ON events FOR EACH ROW EXECUTE FUNCTION reject_change();
CREATE TRIGGER immutable_decisions BEFORE UPDATE OR DELETE ON decisions FOR EACH ROW EXECUTE FUNCTION reject_change();
CREATE TRIGGER immutable_reviews BEFORE UPDATE OR DELETE ON reviews FOR EACH ROW EXECUTE FUNCTION reject_change();
