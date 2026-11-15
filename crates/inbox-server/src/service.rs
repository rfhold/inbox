use crate::{pb::*, validation::*};
use chrono::{DateTime, Utc};
use mail_parser::MessageParser;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Row, postgres::PgRow};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Clone)]
pub struct MailService {
    pub auth: crate::auth::Auth,
    pub principal: Option<crate::auth::Principal>,
    pub db: PgPool,
    pub bucket: Arc<s3::Bucket>,
    pub domain: String,
    pub pipeline: Option<Arc<crate::pipeline::Pipeline>>,
}
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Content {
    pub from: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub subject: String,
    pub text: String,
    pub internet_id: String,
    pub reply_id: String,
    pub references: Vec<String>,
    pub reply_address: String,
    pub sent_at: String,
}
fn internal(_: impl std::fmt::Display) -> Status {
    Status::unavailable("storage temporarily unavailable")
}
fn ts(r: &PgRow, k: &str) -> String {
    r.get::<DateTime<Utc>, _>(k).to_rfc3339()
}
fn uid(r: &PgRow, k: &str) -> String {
    r.get::<Uuid, _>(k).to_string()
}
pub(crate) fn mailbox(r: &PgRow) -> Mailbox {
    Mailbox {
        id: uid(r, "id"),
        local_part: r.get("local_part"),
        name: r.get("name"),
        address: format!(
            "{}@{}",
            r.get::<String, _>("local_part"),
            r.get::<String, _>("domain")
        ),
        created_at: ts(r, "created_at"),
        domain_id: uid(r, "domain_id"),
        domain: r.get("domain"),
    }
}
pub(crate) fn content(r: &PgRow) -> Result<Content, Status> {
    serde_json::from_value(r.get("content")).map_err(internal)
}
pub(crate) fn entry(r: &PgRow) -> Result<MessageEntry, Status> {
    let c = content(r)?;
    Ok(MessageEntry {
        id: uid(r, "id"),
        mailbox_id: uid(r, "mailbox_id"),
        message_id: uid(r, "message_id"),
        folder: r.get("folder"),
        is_read: r.get("is_read"),
        from_address: c.from,
        to: c.to,
        cc: c.cc,
        subject: c.subject,
        preview: c.text.chars().take(200).collect(),
        created_at: ts(r, "created_at"),
        sent_at: c.sent_at,
        raw_size_bytes: r.get::<i64, _>("raw_size") as u64,
    })
}
pub fn parse(raw: &[u8]) -> Result<Content, Status> {
    if raw.len() > MAX_RAW {
        return Err(Status::resource_exhausted("message exceeds 25 MiB"));
    }
    let m = MessageParser::default()
        .parse(raw)
        .ok_or_else(|| Status::invalid_argument("invalid MIME message"))?;
    let addrs = |a: Option<&mail_parser::Address<'_>>| -> Vec<String> {
        a.map(|a| {
            a.iter()
                .filter_map(|x| x.address().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
    };
    let from = addrs(m.from()).into_iter().next().unwrap_or_default();
    let reply_address = addrs(m.reply_to())
        .into_iter()
        .next()
        .unwrap_or_else(|| from.clone());
    let text = m
        .text_bodies()
        .find_map(|p| {
            if let mail_parser::PartType::Text(t) = &p.body {
                Some(t.to_string())
            } else {
                None
            }
        })
        .unwrap_or_default();
    let references = match m.references() {
        mail_parser::HeaderValue::Text(s) => vec![s.to_string()],
        mail_parser::HeaderValue::TextList(v) => v.iter().map(|s| s.to_string()).collect(),
        _ => Vec::new(),
    };
    Ok(Content {
        from,
        to: addrs(m.to()),
        cc: addrs(m.cc()),
        subject: m.subject().unwrap_or_default().into(),
        text,
        internet_id: m.message_id().unwrap_or_default().into(),
        reply_id: m.in_reply_to().as_text().unwrap_or_default().into(),
        references,
        reply_address,
        sent_at: m
            .date()
            .and_then(|d| DateTime::from_timestamp(d.to_timestamp(), 0))
            .map(|d| d.to_rfc3339())
            .unwrap_or_default(),
    })
}
pub(crate) async fn submission_in(db: &mut PgConnection, id: Uuid) -> Result<Submission, Status> {
    let r = sqlx::query("SELECT * FROM submissions WHERE id=$1")
        .bind(id)
        .fetch_optional(&mut *db)
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::not_found("submission not found"))?;
    let rows = sqlx::query("SELECT d.*,m.owner_id=(SELECT m2.owner_id FROM mailboxes m2 WHERE m2.id=$2) AS same_owner FROM deliveries d JOIN mailboxes m ON m.id=d.mailbox_id WHERE submission_id=$1 ORDER BY address")
        .bind(id)
        .bind(r.get::<Option<Uuid>,_>("from_mailbox_id"))
        .fetch_all(db)
        .await
        .map_err(internal)?;
    let recipients: Vec<RecipientDelivery> = rows
        .iter()
        .map(|d| {
            let status: i32 = d.get("status");
            RecipientDelivery {
                address: d.get("address"),
                status,
                mailbox_id: if status == 2 && d.get::<Option<bool>, _>("same_owner") == Some(true) {
                    uid(d, "mailbox_id")
                } else {
                    String::new()
                },
                entry_id: if d.get::<Option<bool>, _>("same_owner") == Some(true) {
                    d.get::<Option<Uuid>, _>("entry_id")
                        .map(|v| v.to_string())
                        .unwrap_or_default()
                } else {
                    String::new()
                },
                error_code: if status == 3 {
                    "DELIVERY_RETRIES_EXHAUSTED".into()
                } else {
                    String::new()
                },
                error_message: if status == 3 {
                    "Local delivery could not be completed".into()
                } else {
                    String::new()
                },
                updated_at: ts(d, "updated_at"),
            }
        })
        .collect();
    let status = if recipients.iter().any(|r| r.status == 1) {
        1
    } else if recipients.iter().all(|r| r.status == 2) {
        2
    } else if recipients.iter().all(|r| r.status == 3) {
        4
    } else {
        3
    };
    Ok(Submission {
        id: id.to_string(),
        message_id: uid(&r, "message_id"),
        from_mailbox_id: r
            .get::<Option<Uuid>, _>("from_mailbox_id")
            .map(|v| v.to_string())
            .unwrap_or_default(),
        status,
        recipients,
        created_at: ts(&r, "created_at"),
        updated_at: ts(&r, "updated_at"),
    })
}
impl MailService {
    pub(crate) fn cursor_namespace(&self) -> &str {
        self.pipeline
            .as_ref()
            .map_or("INBOX", |pipeline| pipeline.namespace())
    }
    pub fn principal<T>(&self, r: &Request<T>) -> Result<crate::auth::Principal, Status> {
        r.extensions()
            .get::<crate::auth::Principal>()
            .cloned()
            .or_else(|| self.principal.clone())
            .or_else(|| (!self.auth.enabled()).then(crate::auth::Principal::local))
            .filter(|p| p.expires_at > chrono::Utc::now())
            .ok_or_else(crate::auth::unauth)
    }
    pub async fn owns(&self, actor: &crate::auth::Actor, mailbox: Uuid) -> Result<(), Status> {
        crate::mutation::owns(
            &mut *self.db.acquire().await.map_err(internal)?,
            actor,
            mailbox,
        )
        .await
    }
    pub async fn visible_change(
        &self,
        actor: &crate::auth::Actor,
        event: &mut ChangeEvent,
    ) -> Result<bool, Status> {
        if [1, 5, 6].contains(&event.kind) {
            return Ok(true);
        }
        let mailbox_visible = if !event.mailbox_id.is_empty() {
            self.owns(actor, id(&event.mailbox_id)?).await.is_ok()
        } else {
            false
        };
        let submission_visible = if !event.submission_id.is_empty() {
            sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM submissions s JOIN mailboxes m ON m.id=s.from_mailbox_id WHERE s.id=$1 AND m.owner_id=$2)")
                .bind(id(&event.submission_id)?).bind(actor.user_id).fetch_one(&self.db).await.map_err(internal)?
        } else {
            false
        };
        if !mailbox_visible {
            event.mailbox_id.clear();
            event.entry_id.clear();
        }
        if !submission_visible {
            event.submission_id.clear();
        }
        Ok(mailbox_visible || submission_visible)
    }
    pub fn broker(&self) -> Result<&Arc<crate::pipeline::Pipeline>, Status> {
        self.pipeline
            .as_ref()
            .ok_or_else(|| Status::unavailable("command broker unavailable"))
    }
    async fn exists(&self, mailbox: Uuid) -> Result<(), Status> {
        if !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM mailboxes WHERE id=$1)")
            .bind(mailbox)
            .fetch_one(&self.db)
            .await
            .map_err(internal)?
        {
            return Err(Status::not_found("mailbox not found"));
        }
        Ok(())
    }
    pub async fn recipient(&self, a: &str) -> Result<(Uuid, String), Status> {
        crate::domains::recipient_in(&mut *self.db.acquire().await.map_err(internal)?, a).await
    }
    async fn fetch_entry(&self, mailbox: Uuid, e: Uuid) -> Result<PgRow, Status> {
        sqlx::query("SELECT e.*,m.content,m.raw_size,m.object_key FROM mailbox_entries e JOIN messages m ON m.id=e.message_id WHERE e.mailbox_id=$1 AND e.id=$2").bind(mailbox).bind(e).fetch_optional(&self.db).await.map_err(internal)?.ok_or_else(||Status::not_found("entry not found"))
    }
    pub async fn submission(&self, id: Uuid) -> Result<Submission, Status> {
        submission_in(&mut *self.db.acquire().await.map_err(internal)?, id).await
    }
    #[tracing::instrument(name="smtp.accept",skip_all,fields(trace_id=tracing::field::Empty,span_id=tracing::field::Empty))]
    pub async fn accept_smtp(
        &self,
        raw: Vec<u8>,
        recipients: Vec<(Uuid, String)>,
    ) -> Result<Uuid, Status> {
        let (trace_id, span_id) = crate::telemetry::ids();
        tracing::Span::current()
            .record("trace_id", trace_id)
            .record("span_id", span_id);
        let response = self
            .broker()?
            .execute(
                crate::mutation::Command::Smtp {
                    raw,
                    recipients: recipients.into_iter().map(|(_, a)| a).collect(),
                },
                crate::telemetry::parent_header(),
            )
            .await?;
        Uuid::from_slice(&response).map_err(internal)
    }
    async fn mutation<M: prost::Message + Default>(
        &self,
        command: crate::mutation::Command,
        parent: String,
    ) -> Result<Response<M>, Status> {
        let bytes = self.broker()?.execute(command, parent).await?;
        Ok(Response::new(
            M::decode(bytes.as_slice()).map_err(internal)?,
        ))
    }
}
fn parent<T>(r: &Request<T>) -> String {
    let current = crate::telemetry::parent_header();
    if !current.is_empty() {
        current
    } else {
        r.metadata()
            .get("traceparent")
            .and_then(|v| v.to_str().ok())
            .filter(|s| s.len() <= 512)
            .unwrap_or_default()
            .to_string()
    }
}
#[tonic::async_trait]
impl mail_service_server::MailService for MailService {
    async fn create_domain(
        &self,
        r: Request<CreateDomainRequest>,
    ) -> Result<Response<CreateDomainResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        actor.require_admin()?;
        let p = parent(&r);
        self.mutation(
            crate::mutation::Command::authorized(
                actor,
                crate::mutation::Command::CreateDomain(r.into_inner().encode_to_vec()),
            ),
            p,
        )
        .await
    }
    async fn set_domain_policy(
        &self,
        r: Request<SetDomainPolicyRequest>,
    ) -> Result<Response<SetDomainPolicyResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        actor.require_admin()?;
        let p = parent(&r);
        self.mutation(
            crate::mutation::Command::authorized(
                actor,
                crate::mutation::Command::SetDomainPolicy(r.into_inner().encode_to_vec()),
            ),
            p,
        )
        .await
    }
    async fn list_domains(
        &self,
        r: Request<ListDomainsRequest>,
    ) -> Result<Response<ListDomainsResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        let mut r = r.into_inner();
        r.page_token =
            self.auth
                .unbind_cursor(self.cursor_namespace(), &actor, "domains", &r.page_token)?;
        let n = page(r.page_size)?;
        let q = format!("domains:{n}");
        let c = decode(&r.page_token, &q)?;
        let rows = sqlx::query("SELECT * FROM domains WHERE ($1::timestamptz IS NULL OR (created_at,id)<($1,$2)) ORDER BY created_at DESC,id DESC LIMIT $3")
            .bind(c.as_ref().map(|c|c.at)).bind(c.as_ref().map(|c|c.id)).bind(n+1).fetch_all(&self.db).await.map_err(internal)?;
        let next_page_token = if rows.len() > n as usize {
            let last = &rows[n as usize - 1];
            encode(&q, last.get("created_at"), last.get("id"))
        } else {
            String::new()
        };
        Ok(Response::new(ListDomainsResponse {
            domains: rows
                .iter()
                .take(n as usize)
                .map(crate::domains::projection)
                .collect(),
            next_page_token: if next_page_token.is_empty() {
                next_page_token
            } else {
                self.auth
                    .bind_cursor(self.cursor_namespace(), &actor, "domains", &next_page_token)
            },
        }))
    }
    async fn create_mailbox(
        &self,
        r: Request<CreateMailboxRequest>,
    ) -> Result<Response<CreateMailboxResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        let p = parent(&r);
        self.mutation(
            crate::mutation::Command::authorized(
                actor,
                crate::mutation::Command::Create(r.into_inner().encode_to_vec()),
            ),
            p,
        )
        .await
    }
    async fn set_read_state(
        &self,
        r: Request<SetReadStateRequest>,
    ) -> Result<Response<SetReadStateResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        let p = parent(&r);
        self.mutation(
            crate::mutation::Command::authorized(
                actor,
                crate::mutation::Command::Read(r.into_inner().encode_to_vec()),
            ),
            p,
        )
        .await
    }
    async fn submit_message(
        &self,
        r: Request<SubmitMessageRequest>,
    ) -> Result<Response<SubmitMessageResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        let p = parent(&r);
        self.mutation(
            crate::mutation::Command::authorized(
                actor,
                crate::mutation::Command::Submit(r.into_inner().encode_to_vec()),
            ),
            p,
        )
        .await
    }
    async fn list_mailboxes(
        &self,
        r: Request<ListMailboxesRequest>,
    ) -> Result<Response<ListMailboxesResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        let mut r = r.into_inner();
        r.page_token =
            self.auth
                .unbind_cursor(self.cursor_namespace(), &actor, "mailboxes", &r.page_token)?;
        let n = page(r.page_size)?;
        let q = format!("mailboxes:{}:{n}", actor.user_id);
        let c = decode(&r.page_token, &q)?;
        let rows=sqlx::query("SELECT m.*,d.name AS domain FROM mailboxes m JOIN domains d ON d.id=m.domain_id WHERE m.owner_id=$4 AND ($1::timestamptz IS NULL OR (m.created_at,m.id)<($1,$2)) ORDER BY m.created_at DESC,m.id DESC LIMIT $3").bind(c.as_ref().map(|c|c.at)).bind(c.as_ref().map(|c|c.id)).bind(n+1).bind(actor.user_id).fetch_all(&self.db).await.map_err(internal)?;
        let next = if rows.len() > n as usize {
            let last = &rows[n as usize - 1];
            encode(&q, last.get("created_at"), last.get("id"))
        } else {
            String::new()
        };
        Ok(Response::new(ListMailboxesResponse {
            mailboxes: rows.iter().take(n as usize).map(mailbox).collect(),
            next_page_token: if next.is_empty() {
                next
            } else {
                self.auth
                    .bind_cursor(self.cursor_namespace(), &actor, "mailboxes", &next)
            },
        }))
    }
    async fn list_messages(
        &self,
        r: Request<ListMessagesRequest>,
    ) -> Result<Response<ListMessagesResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        let mut r = r.into_inner();
        r.page_token =
            self.auth
                .unbind_cursor(self.cursor_namespace(), &actor, "messages", &r.page_token)?;
        let mid = id(&r.mailbox_id)?;
        self.owns(&actor, mid).await?;
        self.exists(mid).await?;
        if ![1, 2].contains(&r.folder) {
            return Err(Status::invalid_argument("folder required"));
        }
        let n = page(r.page_size)?;
        let q = format!("entries:{}:{mid}:{}:{n}", actor.user_id, r.folder);
        let c = decode(&r.page_token, &q)?;
        let rows=sqlx::query("SELECT e.*,m.content,m.raw_size FROM mailbox_entries e JOIN messages m ON m.id=e.message_id WHERE e.mailbox_id=$1 AND e.folder=$2 AND ($3::timestamptz IS NULL OR (e.created_at,e.id)<($3,$4)) ORDER BY e.created_at DESC,e.id DESC LIMIT $5").bind(mid).bind(r.folder).bind(c.as_ref().map(|c|c.at)).bind(c.as_ref().map(|c|c.id)).bind(n+1).fetch_all(&self.db).await.map_err(internal)?;
        let next = if rows.len() > n as usize {
            let last = &rows[n as usize - 1];
            encode(&q, last.get("created_at"), last.get("id"))
        } else {
            String::new()
        };
        Ok(Response::new(ListMessagesResponse {
            entries: rows
                .iter()
                .take(n as usize)
                .map(entry)
                .collect::<Result<_, _>>()?,
            next_page_token: if next.is_empty() {
                next
            } else {
                self.auth
                    .bind_cursor(self.cursor_namespace(), &actor, "messages", &next)
            },
        }))
    }
    async fn get_message(
        &self,
        r: Request<GetMessageRequest>,
    ) -> Result<Response<GetMessageResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        let r = r.into_inner();
        self.owns(&actor, id(&r.mailbox_id)?).await?;
        let row = self
            .fetch_entry(id(&r.mailbox_id)?, id(&r.entry_id)?)
            .await?;
        let c = content(&row)?;
        Ok(Response::new(GetMessageResponse {
            message: Some(Message {
                entry: Some(entry(&row)?),
                text_body: c.text,
                internet_message_id: c.internet_id,
                in_reply_to: c.reply_id,
                references: c.references,
                reply_to_address: c.reply_address,
            }),
        }))
    }
    async fn get_raw_message(
        &self,
        r: Request<GetRawMessageRequest>,
    ) -> Result<Response<GetRawMessageResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        let r = r.into_inner();
        self.owns(&actor, id(&r.mailbox_id)?).await?;
        let row = self
            .fetch_entry(id(&r.mailbox_id)?, id(&r.entry_id)?)
            .await?;
        let raw = self
            .bucket
            .get_object(row.get::<&str, _>("object_key"))
            .await
            .map_err(internal)?;
        if raw.status_code() != 200 {
            return Err(Status::unavailable("archive unavailable"));
        }
        Ok(Response::new(GetRawMessageResponse {
            raw_message: raw.to_vec(),
        }))
    }
    async fn get_submission(
        &self,
        r: Request<GetSubmissionRequest>,
    ) -> Result<Response<GetSubmissionResponse>, Status> {
        let actor = self.principal(&r)?.actor;
        let sid = id(&r.into_inner().submission_id)?;
        let visible=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM submissions s JOIN mailboxes m ON m.id=s.from_mailbox_id WHERE s.id=$1 AND m.owner_id=$2)")
            .bind(sid).bind(actor.user_id).fetch_one(&self.db).await.map_err(internal)?;
        if !visible {
            return Err(Status::not_found("submission not found"));
        }
        Ok(Response::new(GetSubmissionResponse {
            submission: Some(self.submission(sid).await?),
        }))
    }
    type WatchChangesStream = crate::pipeline::ChangeStream;
    async fn watch_changes(
        &self,
        r: Request<WatchChangesRequest>,
    ) -> Result<Response<Self::WatchChangesStream>, Status> {
        use futures_util::StreamExt;
        let principal = self.principal(&r)?;
        let actor = principal.actor.clone();
        let resume = self.auth.unbind_cursor(
            self.cursor_namespace(),
            &actor,
            "changes",
            &r.into_inner().resume_token,
        )?;
        let source = self.broker()?.watch(resume).await?;
        let this = self.clone();
        let filtered = source.filter_map(move |event| {
            let this = this.clone();
            let actor = actor.clone();
            async move {
                match event {
                    Ok(mut event) => match this.visible_change(&actor, &mut event).await {
                        Ok(true) => {
                            event.resume_token = this.auth.bind_cursor(
                                this.cursor_namespace(),
                                &actor,
                                "changes",
                                &event.resume_token,
                            );
                            Some(Ok(event))
                        }
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    },
                    Err(e) => Some(Err(e)),
                }
            }
        });
        let auth = self.auth.clone();
        Ok(Response::new(Box::pin(
            filtered.take_until(async move { principal.ended(&auth).await }),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mime_is_text_only_and_preserves_reply_headers() {
        let raw=b"From: sender@example.org\r\nReply-To: preferred@example.org\r\nTo: user@inbox.test\r\nMessage-ID: <three@example.org>\r\nIn-Reply-To: <two@example.org>\r\nReferences: <one@example.org> <two@example.org>\r\nDate: invalid\r\nSubject: =?UTF-8?B?SGVsbG8=?=\r\nContent-Type: multipart/alternative; boundary=parts\r\n\r\n--parts\r\nContent-Type: text/plain\r\n\r\nplain\r\n--parts\r\nContent-Type: text/html\r\n\r\n<script>never render</script>\r\n--parts--\r\n";
        let c = parse(raw).unwrap();
        assert_eq!(c.subject, "Hello");
        assert_eq!(c.reply_address, "preferred@example.org");
        assert_eq!(c.reply_id, "two@example.org");
        assert_eq!(c.references, vec!["one@example.org", "two@example.org"]);
        assert!(!c.text.contains("script"));
        assert!(c.sent_at.is_empty());
    }
    #[test]
    fn html_only_has_no_text() {
        assert!(
            parse(b"From: sender@example.org\r\nContent-Type: text/html\r\n\r\n<b>HTML only</b>")
                .unwrap()
                .text
                .is_empty()
        )
    }
    #[test]
    fn raw_limit_is_checked_before_parsing() {
        assert_eq!(
            parse(&vec![b'x'; MAX_RAW + 1]).err().unwrap().code(),
            tonic::Code::ResourceExhausted
        )
    }
    #[test]
    fn builder_roundtrip_reply_and_single_reference() {
        let raw = mail_builder::MessageBuilder::new()
            .from("a@inbox.test")
            .to("b@inbox.test")
            .message_id("reply@inbox.test")
            .in_reply_to("source@inbox.test")
            .references(vec!["source@inbox.test"])
            .text_body("plain")
            .write_to_vec()
            .unwrap();
        let c = parse(&raw).unwrap();
        assert_eq!(c.reply_id, "source@inbox.test");
        assert_eq!(c.references, vec!["source@inbox.test"])
    }
    #[test]
    fn idempotency_hash_includes_all_payload_fields() {
        let r = SubmitMessageRequest {
            subject: "one".into(),
            ..Default::default()
        };
        let mut changed = r.clone();
        changed.text_body = "two".into();
        assert_ne!(
            crate::mutation::hash(&r.encode_to_vec()),
            crate::mutation::hash(&changed.encode_to_vec())
        )
    }
}
