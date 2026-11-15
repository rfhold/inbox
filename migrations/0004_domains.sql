CREATE TABLE domains (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE,
    receive_enabled bool NOT NULL,
    send_enabled bool NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE domain_defaults (
    singleton bool PRIMARY KEY DEFAULT true CHECK (singleton),
    domain_id uuid NOT NULL REFERENCES domains
);
ALTER TABLE mailboxes DROP CONSTRAINT mailboxes_local_part_key;
ALTER TABLE mailboxes ADD COLUMN domain_id uuid NOT NULL REFERENCES domains;
ALTER TABLE mailboxes ADD CONSTRAINT mailboxes_domain_local_key UNIQUE(domain_id,local_part);
