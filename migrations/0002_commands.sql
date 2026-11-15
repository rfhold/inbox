CREATE TABLE command_journal (
    id uuid PRIMARY KEY,
    payload_hash text NOT NULL,
    result bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE mutation_requests (
    scope text PRIMARY KEY,
    payload_hash text NOT NULL,
    result bytea NOT NULL
);
CREATE TABLE command_outbox (
    id uuid PRIMARY KEY,
    subject text NOT NULL,
    payload bytea NOT NULL,
    traceparent text NOT NULL DEFAULT '',
    published boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX command_outbox_pending ON command_outbox(created_at) WHERE NOT published;
CREATE TABLE delivery_commands (
    delivery_id uuid PRIMARY KEY REFERENCES deliveries(id),
    command_id uuid NOT NULL UNIQUE
);
