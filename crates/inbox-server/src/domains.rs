use crate::{
    mutation::{Command, storage},
    pb::*,
    service::MailService,
    validation::*,
};
use prost::Message as _;
use sqlx::{PgConnection, Row, postgres::PgRow};
use tonic::Status;
use uuid::Uuid;

pub fn projection(row: &PgRow) -> Domain {
    Domain {
        id: row.get::<Uuid, _>("id").to_string(),
        name: row.get("name"),
        receive_enabled: row.get("receive_enabled"),
        send_enabled: row.get("send_enabled"),
        created_at: row
            .get::<chrono::DateTime<chrono::Utc>, _>("created_at")
            .to_rfc3339(),
        updated_at: row
            .get::<chrono::DateTime<chrono::Utc>, _>("updated_at")
            .to_rfc3339(),
    }
}

pub(crate) async fn lock_policies(tx: &mut PgConnection) -> Result<(), Status> {
    sqlx::query("SELECT id FROM domains ORDER BY id FOR SHARE")
        .fetch_all(tx)
        .await
        .map_err(storage)?;
    Ok(())
}

pub(crate) async fn recipient_in(tx: &mut PgConnection, a: &str) -> Result<(Uuid, String), Status> {
    let a = normalized_address(a)?;
    let (local, domain) = a.split_once('@').expect("validated");
    let row = sqlx::query("SELECT m.id,d.receive_enabled FROM mailboxes m JOIN domains d ON d.id=m.domain_id WHERE m.local_part=$1 AND d.name=$2")
        .bind(local).bind(domain).fetch_optional(tx).await.map_err(storage)?
        .ok_or_else(|| Status::invalid_argument("unknown recipient"))?;
    if !row.get::<bool, _>("receive_enabled") {
        return Err(Status::failed_precondition("domain receiving disabled"));
    }
    Ok((row.get("id"), a))
}

impl MailService {
    pub async fn bootstrap_domains(&self) -> Result<(), Status> {
        self.broker()?
            .execute(Command::BootstrapDomain(self.domain.clone()), String::new())
            .await?;
        Ok(())
    }
}

pub(crate) async fn apply(
    tx: &mut PgConnection,
    command: Command,
    parent: &str,
) -> Result<Vec<u8>, Status> {
    let (row, policy) = match command {
        Command::BootstrapDomain(name) => {
            sqlx::query("SELECT pg_advisory_xact_lock(674346217)")
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            if sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM domain_defaults)")
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?
            {
                return Ok(Vec::new());
            }
            let name = domain(&name)?;
            let row = sqlx::query("INSERT INTO domains(id,name,receive_enabled,send_enabled) VALUES($1,$2,true,true) RETURNING *")
                .bind(Uuid::new_v4()).bind(name).fetch_one(&mut *tx).await.map_err(storage)?;
            sqlx::query("INSERT INTO domain_defaults(domain_id) VALUES($1)")
                .bind(row.get::<Uuid, _>("id"))
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            (row, false)
        }
        Command::CreateDomain(bytes) => {
            let r = CreateDomainRequest::decode(bytes.as_slice()).map_err(storage)?;
            let row = sqlx::query("INSERT INTO domains(id,name,receive_enabled,send_enabled) VALUES($1,$2,$3,$4) RETURNING *")
                .bind(Uuid::new_v4()).bind(domain(&r.name)?).bind(r.receive_enabled).bind(r.send_enabled)
                .fetch_one(&mut *tx).await.map_err(|e| if e.as_database_error().is_some_and(|e| e.is_unique_violation()) { Status::already_exists("domain already exists") } else {storage(e)})?;
            (row, false)
        }
        Command::SetDomainPolicy(bytes) => {
            let r = SetDomainPolicyRequest::decode(bytes.as_slice()).map_err(storage)?;
            let row = sqlx::query("UPDATE domains SET receive_enabled=$2,send_enabled=$3,updated_at=now() WHERE id=$1 RETURNING *")
                .bind(id(&r.domain_id)?).bind(r.receive_enabled).bind(r.send_enabled).fetch_optional(&mut *tx).await.map_err(storage)?
                .ok_or_else(|| Status::not_found("domain not found"))?;
            (row, true)
        }
        _ => return Err(Status::internal("invalid domain command")),
    };
    let d = projection(&row);
    crate::mutation::outbox(
        tx,
        "INBOX.changes",
        ChangeEvent {
            kind: 6,
            domain_id: d.id.clone(),
            ..Default::default()
        }
        .encode_to_vec(),
        parent,
    )
    .await?;
    Ok(if policy {
        SetDomainPolicyResponse { domain: Some(d) }.encode_to_vec()
    } else {
        CreateDomainResponse { domain: Some(d) }.encode_to_vec()
    })
}
