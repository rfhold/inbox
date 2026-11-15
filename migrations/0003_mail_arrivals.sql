CREATE TABLE mail_arrival_generation (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    generation uuid NOT NULL DEFAULT gen_random_uuid()
);
INSERT INTO mail_arrival_generation(singleton) VALUES (true);

CREATE TABLE mail_arrival_watermarks (
    mailbox_id uuid PRIMARY KEY REFERENCES mailboxes(id),
    position bigint NOT NULL CHECK (position > 0)
);

CREATE TABLE mail_arrivals (
    entry_id uuid PRIMARY KEY REFERENCES mailbox_entries(id),
    mailbox_id uuid NOT NULL REFERENCES mailboxes(id),
    position bigint NOT NULL CHECK (position > 0),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (mailbox_id, position)
);
