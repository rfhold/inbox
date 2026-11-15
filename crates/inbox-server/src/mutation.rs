use crate::{
    pb::*,
    service::{MailService, parse},
    validation::*,
};
use mail_builder::MessageBuilder;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, Row};
use std::collections::BTreeSet;
use tonic::Status;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub enum Command {
    Create(Vec<u8>),
    Read(Vec<u8>),
    Submit(Vec<u8>),
    Smtp {
        raw: Vec<u8>,
        recipients: Vec<String>,
    },
    Deliver(Uuid),
    BootstrapDomain(String),
    CreateDomain(Vec<u8>),
    SetDomainPolicy(Vec<u8>),
    Authorized {
        actor: crate::auth::Actor,
        command: Box<Command>,
    },
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub code: i32,
    pub message: String,
    pub response: Vec<u8>,
}
impl Outcome {
    pub fn from_result(result: Result<Vec<u8>, Status>) -> Self {
        match result {
            Ok(response) => Self {
                code: 0,
                message: String::new(),
                response,
            },
            Err(s) => Self {
                code: s.code() as i32,
                message: s.message().into(),
                response: Vec::new(),
            },
        }
    }
    pub fn result(self) -> Result<Vec<u8>, Status> {
        if self.code == 0 {
            Ok(self.response)
        } else {
            Err(Status::new(tonic::Code::from_i32(self.code), self.message))
        }
    }
}
pub fn storage(_: impl std::fmt::Display) -> Status {
    Status::unavailable("storage temporarily unavailable")
}
pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

impl Command {
    pub fn request_digest(&self) -> Result<String, Status> {
        if let Self::Authorized { actor, command } = self {
            let mut bytes = actor.user_id.as_bytes().to_vec();
            bytes.extend(bincode::serialize(command).map_err(storage)?);
            Ok(hash(&bytes))
        } else {
            Ok(hash(&bincode::serialize(self).map_err(storage)?))
        }
    }
    pub fn authorized(actor: crate::auth::Actor, command: Command) -> Self {
        Self::Authorized {
            actor,
            command: Box::new(command),
        }
    }
    pub fn request_scope(&self) -> Result<Option<String>, Status> {
        if let Self::Authorized { actor, command } = self {
            if matches!(
                command.as_ref(),
                Self::Authorized { .. }
                    | Self::Smtp { .. }
                    | Self::Deliver(_)
                    | Self::BootstrapDomain(_)
            ) {
                return Err(Status::permission_denied("internal command forbidden"));
            }
            return command
                .request_scope()
                .map(|scope| scope.map(|s| format!("user:{}:{s}", actor.user_id)));
        }
        let (kind, key) = match self {
            Self::CreateDomain(bytes) => (
                "create_domain",
                CreateDomainRequest::decode(bytes.as_slice())
                    .map_err(storage)?
                    .request_id,
            ),
            Self::SetDomainPolicy(bytes) => (
                "domain_policy",
                SetDomainPolicyRequest::decode(bytes.as_slice())
                    .map_err(storage)?
                    .request_id,
            ),
            Self::Create(bytes) => (
                "create",
                CreateMailboxRequest::decode(bytes.as_slice())
                    .map_err(storage)?
                    .request_id,
            ),
            Self::Read(bytes) => (
                "read",
                SetReadStateRequest::decode(bytes.as_slice())
                    .map_err(storage)?
                    .request_id,
            ),
            _ => return Ok(None),
        };
        if key.is_empty() {
            if matches!(self, Self::CreateDomain(_) | Self::SetDomainPolicy(_)) {
                return Err(Status::invalid_argument("request ID required"));
            }
            return Ok(None);
        }
        if key.len() > 256 || key.chars().any(char::is_control) {
            return Err(Status::invalid_argument("invalid request ID"));
        }
        header(&key)?;
        Ok(Some(format!("{kind}:{key}")))
    }
}

pub(crate) async fn owns(
    tx: &mut PgConnection,
    actor: &crate::auth::Actor,
    mailbox: Uuid,
) -> Result<(), Status> {
    if sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM mailboxes WHERE id=$1 AND owner_id=$2)",
    )
    .bind(mailbox)
    .bind(actor.user_id)
    .fetch_one(tx)
    .await
    .map_err(storage)?
    {
        Ok(())
    } else {
        Err(Status::not_found("mailbox not found"))
    }
}

pub async fn outbox(
    tx: &mut PgConnection,
    subject: &str,
    payload: Vec<u8>,
    parent: &str,
) -> Result<(), Status> {
    sqlx::query("INSERT INTO command_outbox(id,subject,payload,traceparent) VALUES($1,$2,$3,$4)")
        .bind(Uuid::new_v4())
        .bind(subject)
        .bind(payload)
        .bind(parent)
        .execute(tx)
        .await
        .map_err(storage)?;
    Ok(())
}
async fn change(
    tx: &mut PgConnection,
    kind: i32,
    mailbox_id: String,
    entry_id: String,
    submission_id: String,
    parent: &str,
) -> Result<(), Status> {
    outbox(
        tx,
        "INBOX.changes",
        ChangeEvent {
            resume_token: String::new(),
            kind,
            mailbox_id,
            entry_id,
            submission_id,
            domain_id: String::new(),
        }
        .encode_to_vec(),
        parent,
    )
    .await
}

impl MailService {
    pub(crate) async fn apply(
        &self,
        command: Command,
        tx: &mut PgConnection,
        parent: &str,
    ) -> Result<Vec<u8>, Status> {
        let (actor, command) = match command {
            Command::Authorized { actor, command } => {
                if matches!(
                    command.as_ref(),
                    Command::Authorized { .. }
                        | Command::Smtp { .. }
                        | Command::Deliver(_)
                        | Command::BootstrapDomain(_)
                ) {
                    return Err(Status::permission_denied("internal command forbidden"));
                }
                (actor, *command)
            }
            command
            @ (Command::Smtp { .. } | Command::Deliver(_) | Command::BootstrapDomain(_)) => {
                (crate::auth::Actor::local(), command)
            }
            command if !self.auth.enabled() => (crate::auth::Actor::local(), command),
            _ => return Err(crate::auth::unauth()),
        };
        if matches!(
            command,
            Command::CreateDomain(_) | Command::SetDomainPolicy(_)
        ) {
            actor.require_admin()?;
            if !sqlx::query_scalar::<_, bool>("SELECT administrator FROM inbox_users WHERE id=$1")
                .bind(actor.user_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?
                .unwrap_or(false)
            {
                return Err(Status::permission_denied("administrator required"));
            }
        }
        match command {
            Command::Authorized { .. } => Err(Status::permission_denied("nested actor forbidden")),
            c @ (Command::BootstrapDomain(_)
            | Command::CreateDomain(_)
            | Command::SetDomainPolicy(_)) => crate::domains::apply(tx, c, parent).await,
            Command::Create(bytes) => {
                crate::domains::lock_policies(tx).await?;
                let r = CreateMailboxRequest::decode(bytes.as_slice()).map_err(storage)?;
                let l = local(&r.local_part)?;
                header(&r.name)?;
                let domain_id = if r.domain_id.is_empty() {
                    sqlx::query_scalar::<_, Uuid>("SELECT domain_id FROM domain_defaults")
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(storage)?
                        .ok_or_else(|| Status::failed_precondition("default domain unavailable"))?
                } else {
                    id(&r.domain_id)?
                };
                let domain_name =
                    sqlx::query_scalar::<_, String>("SELECT name FROM domains WHERE id=$1")
                        .bind(domain_id)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(storage)?
                        .ok_or_else(|| Status::not_found("domain not found"))?;
                if self.auth.enabled() {
                    let enabled: bool = sqlx::query_scalar(
                        "SELECT receive_enabled OR send_enabled FROM domains WHERE id=$1",
                    )
                    .bind(domain_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
                    if !enabled {
                        return Err(Status::failed_precondition("domain disabled"));
                    }
                }
                let row = sqlx::query(
                    "INSERT INTO mailboxes(id,local_part,name,domain_id,owner_id) VALUES($1,$2,$3,$4,$6) RETURNING *, $5::text AS domain",
                )
                .bind(Uuid::new_v4())
                .bind(l)
                .bind(r.name)
                .bind(domain_id)
                .bind(domain_name)
                .bind(actor.user_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| {
                    if e.as_database_error()
                        .is_some_and(|d| d.is_unique_violation())
                    {
                        Status::already_exists("mailbox already exists")
                    } else {
                        storage(e)
                    }
                })?;
                let m = crate::service::mailbox(&row);
                change(tx, 2, m.id.clone(), String::new(), String::new(), parent).await?;
                Ok(CreateMailboxResponse { mailbox: Some(m) }.encode_to_vec())
            }
            Command::Read(bytes) => {
                let r = SetReadStateRequest::decode(bytes.as_slice()).map_err(storage)?;
                let m = id(&r.mailbox_id)?;
                owns(tx, &actor, m).await?;
                let e = id(&r.entry_id)?;
                if sqlx::query(
                    "UPDATE mailbox_entries SET is_read=$3 WHERE mailbox_id=$1 AND id=$2",
                )
                .bind(m)
                .bind(e)
                .bind(r.is_read)
                .execute(&mut *tx)
                .await
                .map_err(storage)?
                .rows_affected()
                    == 0
                {
                    return Err(Status::not_found("entry not found"));
                }
                let row=sqlx::query("SELECT e.*,m.content,m.raw_size FROM mailbox_entries e JOIN messages m ON m.id=e.message_id WHERE e.mailbox_id=$1 AND e.id=$2")
                    .bind(m).bind(e).fetch_one(&mut *tx).await.map_err(storage)?;
                change(tx, 3, m.to_string(), e.to_string(), String::new(), parent).await?;
                Ok(SetReadStateResponse {
                    entry: Some(crate::service::entry(&row)?),
                }
                .encode_to_vec())
            }
            Command::Submit(bytes) => {
                let r = SubmitMessageRequest::decode(bytes.as_slice()).map_err(storage)?;
                let from = id(&r.from_mailbox_id)?;
                owns(tx, &actor, from).await?;
                if r.idempotency_key.is_empty() || r.idempotency_key.len() > 256 {
                    return Err(Status::invalid_argument(
                        "idempotency key required (max 256 bytes)",
                    ));
                }
                header(&r.idempotency_key)?;
                let digest = hash(&bytes);
                sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
                    .bind(format!("{from}:{}", r.idempotency_key))
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                if let Some(row)=sqlx::query("SELECT id,payload_hash FROM submissions WHERE from_mailbox_id=$1 AND idempotency_key=$2").bind(from).bind(&r.idempotency_key).fetch_optional(&mut *tx).await.map_err(storage)? {
                    if row.get::<String,_>("payload_hash")!=digest {return Err(Status::already_exists("idempotency key used with different payload"))}
                    return Ok(SubmitMessageResponse{submission:Some(crate::service::submission_in(tx,row.get("id")).await?)}.encode_to_vec())
                }
                header(&r.subject)?;
                if r.to.is_empty() || r.to.len() + r.cc.len() > 1000 {
                    return Err(Status::invalid_argument(
                        "To required; maximum 1000 recipients",
                    ));
                }
                if r.text_body.len() > MAX_RAW {
                    return Err(Status::resource_exhausted("message exceeds 25 MiB"));
                }
                crate::domains::lock_policies(tx).await?;
                let sender =
                    sqlx::query("SELECT m.local_part,d.name,d.send_enabled FROM mailboxes m JOIN domains d ON d.id=m.domain_id WHERE m.id=$1")
                        .bind(from)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(storage)?
                        .ok_or_else(|| Status::not_found("mailbox not found"))?;
                if !sender.get::<bool, _>("send_enabled") {
                    return Err(Status::failed_precondition("domain sending disabled"));
                }
                let sender_domain: String = sender.get("name");
                let sender: String = sender.get("local_part");
                let mut recipients = Vec::new();
                for a in r.to.iter().chain(r.cc.iter()) {
                    recipients.push(self.recipient_in(tx, a).await?)
                }
                let to =
                    r.to.iter()
                        .map(|a| normalized_address(a))
                        .collect::<Result<Vec<_>, _>>()?;
                let cc =
                    r.cc.iter()
                        .map(|a| normalized_address(a))
                        .collect::<Result<Vec<_>, _>>()?;
                let mut builder = MessageBuilder::new()
                    .from(format!("{sender}@{sender_domain}"))
                    .to(to)
                    .subject(r.subject)
                    .text_body(r.text_body)
                    .message_id(format!("{}@{sender_domain}", Uuid::new_v4()));
                if !cc.is_empty() {
                    builder = builder.cc(cc)
                }
                match (&r.reply_to_entry_id, &r.source_mailbox_id) {
                    (Some(e), Some(m)) => {
                        owns(tx, &actor, id(m)?).await?;
                        let row=sqlx::query("SELECT m.content FROM mailbox_entries e JOIN messages m ON m.id=e.message_id WHERE e.mailbox_id=$1 AND e.id=$2").bind(id(m)?).bind(id(e)?).fetch_optional(&mut *tx).await.map_err(storage)?.ok_or_else(||Status::not_found("entry not found"))?;
                        let source = crate::service::content(&row)?;
                        if !source.internet_id.is_empty() {
                            header(&source.internet_id)?;
                            builder = builder.in_reply_to(source.internet_id.clone())
                        }
                        let mut refs = source.references;
                        if !source.internet_id.is_empty() {
                            refs.push(source.internet_id)
                        }
                        if !refs.is_empty() {
                            for h in &refs {
                                header(h)?
                            }
                            builder = builder.references(refs)
                        }
                    }
                    (None, None) => {}
                    _ => {
                        return Err(Status::invalid_argument(
                            "reply source fields must both be present",
                        ));
                    }
                }
                let raw = builder.write_to_vec().map_err(storage)?;
                let sid = self
                    .persist_in(
                        tx,
                        raw,
                        Some(from),
                        recipients,
                        Some((&r.idempotency_key, &digest)),
                        parent,
                    )
                    .await?;
                Ok(SubmitMessageResponse {
                    submission: Some(crate::service::submission_in(tx, sid).await?),
                }
                .encode_to_vec())
            }
            Command::Smtp { raw, recipients } => {
                crate::domains::lock_policies(tx).await?;
                if recipients.is_empty() || recipients.len() > 1000 {
                    return Err(Status::invalid_argument("recipients required"));
                }
                let mut resolved = Vec::new();
                for a in recipients {
                    resolved.push(self.recipient_in(tx, &a).await?)
                }
                let sid = self
                    .persist_in(tx, raw, None, resolved, None, parent)
                    .await?;
                Ok(sid.as_bytes().to_vec())
            }
            Command::Deliver(job) => {
                let row = sqlx::query("SELECT * FROM deliveries WHERE id=$1 FOR UPDATE")
                    .bind(job)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("delivery not found"))?;
                if row.get::<i32, _>("status") != 1 {
                    return Ok(Vec::new());
                }
                let sid: Uuid = row.get("submission_id");
                let mid: Uuid = row.get("mailbox_id");
                if row.get::<i32, _>("attempts") >= 8
                    && row
                        .get::<Option<chrono::DateTime<chrono::Utc>>, _>("lease_until")
                        .is_none_or(|lease| lease <= chrono::Utc::now())
                {
                    sqlx::query("UPDATE deliveries SET status=3,lease_until=NULL,updated_at=now() WHERE id=$1").bind(job).execute(&mut *tx).await.map_err(storage)?;
                } else {
                    let inserted:Option<Uuid>=sqlx::query_scalar("INSERT INTO mailbox_entries(id,mailbox_id,message_id,folder) SELECT $1,$2,message_id,1 FROM submissions WHERE id=$3 ON CONFLICT(mailbox_id,message_id,folder) DO NOTHING RETURNING id").bind(Uuid::new_v4()).bind(mid).bind(sid).fetch_optional(&mut *tx).await.map_err(storage)?;
                    let e = if let Some(entry) = inserted {
                        crate::arrivals::record(tx, mid, entry).await?;
                        entry
                    } else {
                        sqlx::query_scalar("SELECT e.id FROM mailbox_entries e JOIN submissions s ON s.message_id=e.message_id WHERE s.id=$1 AND e.mailbox_id=$2 AND e.folder=1")
                            .bind(sid).bind(mid).fetch_one(&mut *tx).await.map_err(storage)?
                    };
                    sqlx::query("UPDATE deliveries SET status=2,entry_id=$2,lease_until=NULL,updated_at=now() WHERE id=$1").bind(job).bind(e).execute(&mut *tx).await.map_err(storage)?;
                    change(
                        tx,
                        3,
                        mid.to_string(),
                        e.to_string(),
                        sid.to_string(),
                        parent,
                    )
                    .await?;
                }
                sqlx::query("UPDATE submissions SET updated_at=now() WHERE id=$1")
                    .bind(sid)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                change(tx, 4, String::new(), String::new(), sid.to_string(), parent).await?;
                Ok(Vec::new())
            }
        }
    }
    async fn recipient_in(&self, tx: &mut PgConnection, a: &str) -> Result<(Uuid, String), Status> {
        crate::domains::recipient_in(tx, a).await
    }
    async fn persist_in(
        &self,
        tx: &mut PgConnection,
        raw: Vec<u8>,
        from: Option<Uuid>,
        recipients: Vec<(Uuid, String)>,
        key: Option<(&str, &str)>,
        parent: &str,
    ) -> Result<Uuid, Status> {
        let c = parse(&raw)?;
        let mid = Uuid::new_v4();
        let sid = Uuid::new_v4();
        let object = format!("raw/{mid}.eml");
        let put = self
            .bucket
            .put_object(&object, &raw)
            .await
            .map_err(storage)?;
        if !(200..300).contains(&put.status_code()) {
            return Err(Status::unavailable("archive unavailable"));
        }
        sqlx::query("INSERT INTO messages(id,object_key,content,raw_size) VALUES($1,$2,$3,$4)")
            .bind(mid)
            .bind(object)
            .bind(serde_json::to_value(c).map_err(storage)?)
            .bind(raw.len() as i64)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        sqlx::query("INSERT INTO submissions(id,message_id,from_mailbox_id,idempotency_key,payload_hash) VALUES($1,$2,$3,$4,$5)").bind(sid).bind(mid).bind(from).bind(key.map(|k|k.0)).bind(key.map(|k|k.1)).execute(&mut *tx).await.map_err(storage)?;
        if let Some(from) = from {
            let entry = Uuid::new_v4();
            sqlx::query("INSERT INTO mailbox_entries(id,mailbox_id,message_id,folder,is_read) VALUES($1,$2,$3,2,true)").bind(entry).bind(from).bind(mid).execute(&mut *tx).await.map_err(storage)?;
            change(
                tx,
                3,
                from.to_string(),
                entry.to_string(),
                sid.to_string(),
                parent,
            )
            .await?;
        }
        let mut seen = BTreeSet::new();
        for (m, a) in recipients {
            if !seen.insert(m) {
                continue;
            }
            let delivery = Uuid::new_v4();
            let command = Uuid::new_v4();
            sqlx::query("INSERT INTO deliveries(id,submission_id,mailbox_id,address,traceparent) VALUES($1,$2,$3,$4,$5)").bind(delivery).bind(sid).bind(m).bind(a).bind(parent).execute(&mut *tx).await.map_err(storage)?;
            sqlx::query("INSERT INTO delivery_commands(delivery_id,command_id) VALUES($1,$2)")
                .bind(delivery)
                .bind(command)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            outbox(
                tx,
                "INBOX.commands.delivery",
                crate::pipeline::Envelope::inline(command, Command::Deliver(delivery), parent)?
                    .encode()?,
                parent,
            )
            .await?;
        }
        change(tx, 4, String::new(), String::new(), sid.to_string(), parent).await?;
        Ok(sid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actors_partition_request_keys_and_cannot_wrap_internal_operations() {
        let a = crate::auth::Actor {
            user_id: Uuid::new_v4(),
            administrator: false,
        };
        let b = crate::auth::Actor {
            user_id: Uuid::new_v4(),
            administrator: false,
        };
        let create = || {
            Command::Create(
                CreateMailboxRequest {
                    request_id: "same-key".into(),
                    ..Default::default()
                }
                .encode_to_vec(),
            )
        };
        let mut elevated = a.clone();
        elevated.administrator = true;
        assert_eq!(
            Command::authorized(a.clone(), create())
                .request_digest()
                .unwrap(),
            Command::authorized(elevated, create())
                .request_digest()
                .unwrap(),
            "role changes do not change the caller's application payload"
        );
        assert_ne!(
            Command::authorized(a.clone(), create())
                .request_scope()
                .unwrap(),
            Command::authorized(b, create()).request_scope().unwrap()
        );
        for internal in [
            Command::Deliver(Uuid::new_v4()),
            Command::BootstrapDomain("inbox.test".into()),
            Command::Smtp {
                raw: vec![],
                recipients: vec![],
            },
            Command::authorized(a.clone(), create()),
        ] {
            assert!(
                Command::authorized(a.clone(), internal)
                    .request_scope()
                    .is_err()
            );
        }
    }

    #[test]
    fn domain_writes_require_scoped_request_keys() {
        let mut r = CreateDomainRequest {
            name: "inbox.test".into(),
            ..Default::default()
        };
        assert_eq!(
            Command::CreateDomain(r.encode_to_vec())
                .request_scope()
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        r.request_id = "same-key".into();
        let create = Command::CreateDomain(r.encode_to_vec())
            .request_scope()
            .unwrap()
            .unwrap();
        let policy = Command::SetDomainPolicy(
            SetDomainPolicyRequest {
                domain_id: Uuid::new_v4().to_string(),
                request_id: r.request_id,
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .request_scope()
        .unwrap()
        .unwrap();
        assert_eq!(create, "create_domain:same-key");
        assert_eq!(policy, "domain_policy:same-key");
        assert_ne!(create, policy);
    }
}
