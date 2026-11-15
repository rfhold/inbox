CREATE TABLE inbox_users (
    id uuid PRIMARY KEY,
    issuer text NOT NULL,
    subject text NOT NULL,
    administrator bool NOT NULL DEFAULT false,
    UNIQUE(issuer,subject)
);
INSERT INTO inbox_users(id,issuer,subject,administrator) VALUES ('00000000-0000-4000-8000-000000000001','inbox:local','workspace',true);
ALTER TABLE mailboxes ADD COLUMN owner_id uuid NOT NULL DEFAULT '00000000-0000-4000-8000-000000000001' REFERENCES inbox_users(id);
ALTER TABLE mailboxes ALTER COLUMN owner_id DROP DEFAULT;
CREATE INDEX mailbox_owner_page ON mailboxes(owner_id,created_at DESC,id DESC);
CREATE FUNCTION inbox_immutable_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.owner_id IS DISTINCT FROM NEW.owner_id THEN
        RAISE EXCEPTION 'mailbox owner is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER mailbox_immutable_owner BEFORE UPDATE ON mailboxes
FOR EACH ROW EXECUTE FUNCTION inbox_immutable_owner();
CREATE TABLE inbox_login_attempts (
    state_hash text PRIMARY KEY,
    correlation_hash text NOT NULL,
    verifier text NOT NULL,
    nonce text NOT NULL,
    return_to text NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE TABLE inbox_sessions (
    token_hash text PRIMARY KEY,
    owner_id uuid NOT NULL REFERENCES inbox_users,
    administrator bool NOT NULL,
    issuer text NOT NULL,
    client_id text NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE INDEX inbox_sessions_expiry ON inbox_sessions(expires_at);
CREATE TABLE inbox_auth_keys (
    purpose text PRIMARY KEY,
    secret bytea NOT NULL CHECK(octet_length(secret)=32)
);
