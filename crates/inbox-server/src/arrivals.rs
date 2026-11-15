//! Retained, ID-only Inbox arrivals. Positions are contiguous within each mailbox.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Row};
use tonic::Status;
use uuid::Uuid;

use crate::mutation::storage;

pub const MAX_PAGE_SIZE: u32 = 100;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArrivalCursor(String);

impl ArrivalCursor {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    generation: Uuid,
    mailbox: Uuid,
    position: i64,
}

impl Cursor {
    fn encode(&self) -> ArrivalCursor {
        ArrivalCursor(
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).expect("cursor serialization")),
        )
    }

    fn decode(token: &str, mailbox: Uuid) -> Result<Self, Status> {
        if token.len() > 512 {
            return Err(Status::invalid_argument("invalid arrival cursor"));
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| Status::invalid_argument("invalid arrival cursor"))?;
        let cursor: Self = serde_json::from_slice(&bytes)
            .map_err(|_| Status::invalid_argument("invalid arrival cursor"))?;
        if cursor.version != 1 || cursor.mailbox != mailbox || cursor.position < 0 {
            return Err(Status::invalid_argument("invalid arrival cursor"));
        }
        Ok(cursor)
    }
}

#[derive(Clone, Debug)]
pub struct Arrival {
    /// The Inbox entry UUID is also the stable event ID.
    pub entry_id: Uuid,
    pub mailbox_id: Uuid,
    pub position: i64,
    pub occurred_at: DateTime<Utc>,
    pub cursor: ArrivalCursor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetReason {
    GenerationChanged,
    FuturePosition,
    ReplayGap,
}

#[derive(Debug)]
pub struct ArrivalPage {
    pub arrivals: Vec<Arrival>,
    pub next_cursor: ArrivalCursor,
    pub truncated: bool,
    /// A reset has no arrivals; its cursor starts a fresh watch at the watermark.
    pub reset: Option<ResetReason>,
}

async fn watermark(db: &mut PgConnection, mailbox: Uuid) -> Result<Cursor, Status> {
    let row = sqlx::query("SELECT g.generation, COALESCE(w.position,0) AS position FROM mailboxes m CROSS JOIN mail_arrival_generation g LEFT JOIN mail_arrival_watermarks w ON w.mailbox_id=m.id WHERE m.id=$1")
        .bind(mailbox).fetch_optional(db).await.map_err(storage)?
        .ok_or_else(|| Status::not_found("mailbox not found"))?;
    Ok(Cursor {
        version: 1,
        generation: row.get("generation"),
        mailbox,
        position: row.get("position"),
    })
}

/// Start at the current committed watermark, without replaying historical mail.
pub async fn load_watermark(db: &PgPool, mailbox: Uuid) -> Result<ArrivalCursor, Status> {
    let mut connection = db.acquire().await.map_err(storage)?;
    Ok(watermark(&mut connection, mailbox).await?.encode())
}

/// Replay strictly after a cursor. Reusing a cursor redelivers stable event IDs.
pub async fn read_after(
    db: &PgPool,
    mailbox: Uuid,
    token: &str,
    limit: u32,
) -> Result<ArrivalPage, Status> {
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(Status::invalid_argument("arrival page size must be 1..100"));
    }
    let mut tx = db.begin().await.map_err(storage)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
    let current = watermark(&mut tx, mailbox).await?;
    let cursor = Cursor::decode(token, mailbox)?;
    let mut reset = if cursor.generation != current.generation {
        Some(ResetReason::GenerationChanged)
    } else if cursor.position > current.position {
        Some(ResetReason::FuturePosition)
    } else {
        None
    };
    let mut arrivals = Vec::new();
    let mut position = cursor.position;
    if reset.is_none() {
        let rows = sqlx::query("SELECT entry_id,position,occurred_at FROM mail_arrivals WHERE mailbox_id=$1 AND position>$2 ORDER BY position LIMIT $3")
            .bind(mailbox).bind(position).bind(i64::from(limit)).fetch_all(&mut *tx).await.map_err(storage)?;
        for row in rows {
            let next: i64 = row.get("position");
            if position.checked_add(1) != Some(next) {
                reset = Some(ResetReason::ReplayGap);
                break;
            }
            position = next;
            arrivals.push(Arrival {
                entry_id: row.get("entry_id"),
                mailbox_id: mailbox,
                position,
                occurred_at: row.get("occurred_at"),
                cursor: Cursor {
                    position,
                    ..current
                }
                .encode(),
            });
        }
        if arrivals.len() < limit as usize && position != current.position {
            reset = Some(ResetReason::ReplayGap);
        }
    }
    tx.commit().await.map_err(storage)?;
    if reset.is_some() {
        arrivals.clear();
        position = current.position;
    }
    Ok(ArrivalPage {
        arrivals,
        next_cursor: Cursor {
            position,
            ..current
        }
        .encode(),
        truncated: reset.is_none() && position < current.position,
        reset,
    })
}

/// Called only by the delivery consumer, in its domain/journal/outbox transaction.
pub(crate) async fn record(
    tx: &mut PgConnection,
    mailbox: Uuid,
    entry: Uuid,
) -> Result<(), Status> {
    // Serialize allocation until commit: sequence allocation alone is not commit ordered.
    sqlx::query("SELECT pg_advisory_xact_lock(7271014791491)")
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
    let position: i64 = sqlx::query_scalar("INSERT INTO mail_arrival_watermarks(mailbox_id,position) VALUES($1,1) ON CONFLICT(mailbox_id) DO UPDATE SET position=mail_arrival_watermarks.position+1 RETURNING position")
        .bind(mailbox).fetch_one(&mut *tx).await.map_err(storage)?;
    sqlx::query("INSERT INTO mail_arrivals(entry_id,mailbox_id,position) VALUES($1,$2,$3)")
        .bind(entry)
        .bind(mailbox)
        .bind(position)
        .execute(tx)
        .await
        .map_err(storage)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires explicitly isolated INBOX_TEST_DATABASE_URL; creates a retained private schema"]
    async fn postgres_order_rollback_and_replay() {
        let url = std::env::var("INBOX_TEST_DATABASE_URL").expect("isolated test database");
        let admin = PgPool::connect(&url).await.unwrap();
        let schema = format!("arrival_test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let db = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .after_connect(move |connection, _| {
                let query = format!("SET search_path TO {schema}");
                Box::pin(async move {
                    sqlx::query(&query).execute(connection).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        sqlx::migrate!("../../migrations").run(&db).await.unwrap();
        let mailbox = Uuid::new_v4();
        let domain = Uuid::new_v4();
        sqlx::query("INSERT INTO domains(id,name,receive_enabled,send_enabled) VALUES($1,'synthetic.test',true,true)").bind(domain).execute(&db).await.unwrap();
        sqlx::query("INSERT INTO mailboxes(id,local_part,name,domain_id,owner_id) VALUES($1,$2,'Synthetic',$3,'00000000-0000-4000-8000-000000000001')")
            .bind(mailbox)
            .bind(mailbox.to_string())
            .bind(domain)
            .execute(&db)
            .await
            .unwrap();
        let initial = load_watermark(&db, mailbox).await.unwrap();
        let mut discarded = db.begin().await.unwrap();
        let discarded_message = Uuid::new_v4();
        let discarded_entry = Uuid::new_v4();
        sqlx::query("INSERT INTO messages(id,object_key,content,raw_size) VALUES($1,$2,'{}',0)")
            .bind(discarded_message)
            .bind(discarded_message.to_string())
            .execute(&mut *discarded)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO mailbox_entries(id,mailbox_id,message_id,folder) VALUES($1,$2,$3,1)",
        )
        .bind(discarded_entry)
        .bind(mailbox)
        .bind(discarded_message)
        .execute(&mut *discarded)
        .await
        .unwrap();
        record(&mut discarded, mailbox, discarded_entry)
            .await
            .unwrap();
        discarded.rollback().await.unwrap();
        assert_eq!(load_watermark(&db, mailbox).await.unwrap(), initial);
        let survivors: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM mailbox_entries WHERE id=$1)+(SELECT count(*) FROM mail_arrivals WHERE entry_id=$1)")
            .bind(discarded_entry).fetch_one(&db).await.unwrap();
        assert_eq!(survivors, 0, "entry and arrival roll back together");
        let mut entries = Vec::new();
        for _ in 0..3 {
            let message = Uuid::new_v4();
            let entry = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO messages(id,object_key,content,raw_size) VALUES($1,$2,'{}',0)",
            )
            .bind(message)
            .bind(message.to_string())
            .execute(&db)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO mailbox_entries(id,mailbox_id,message_id,folder) VALUES($1,$2,$3,1)",
            )
            .bind(entry)
            .bind(mailbox)
            .bind(message)
            .execute(&db)
            .await
            .unwrap();
            entries.push(entry);
        }
        assert!(
            read_after(&db, mailbox, initial.as_str(), 100)
                .await
                .unwrap()
                .arrivals
                .is_empty(),
            "old entries are not backfilled"
        );
        let mut first = db.begin().await.unwrap();
        record(&mut first, mailbox, entries[0]).await.unwrap();
        assert_eq!(
            load_watermark(&db, mailbox).await.unwrap(),
            initial,
            "uncommitted allocation is invisible"
        );
        let mut second = db.begin().await.unwrap();
        let mut blocked = Box::pin(record(&mut second, mailbox, entries[1]));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut blocked)
                .await
                .is_err(),
            "later writer must wait for commit"
        );
        first.commit().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), blocked)
            .await
            .unwrap()
            .unwrap();
        second.commit().await.unwrap();
        let page = read_after(&db, mailbox, initial.as_str(), 1).await.unwrap();
        assert_eq!(page.arrivals[0].entry_id, entries[0]);
        assert!(page.truncated);
        let next = read_after(&db, mailbox, page.next_cursor.as_str(), 100)
            .await
            .unwrap();
        assert_eq!(next.arrivals[0].entry_id, entries[1]);
        assert_eq!(next.arrivals[0].position, 2);
        let again = read_after(&db, mailbox, initial.as_str(), 1).await.unwrap();
        assert_eq!(again.arrivals[0].entry_id, page.arrivals[0].entry_id);
        let mut rollback = db.begin().await.unwrap();
        record(&mut rollback, mailbox, entries[2]).await.unwrap();
        let mut after_rollback = db.begin().await.unwrap();
        let mut waiting = Box::pin(record(&mut after_rollback, mailbox, entries[2]));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiting)
                .await
                .is_err()
        );
        rollback.rollback().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        after_rollback.commit().await.unwrap();
        let third = read_after(&db, mailbox, next.next_cursor.as_str(), 100)
            .await
            .unwrap();
        assert_eq!(
            third.arrivals[0].position, 3,
            "rollback must not consume a position"
        );
        let mut duplicate = db.begin().await.unwrap();
        assert!(record(&mut duplicate, mailbox, entries[2]).await.is_err());
        duplicate.rollback().await.unwrap();
        assert_eq!(
            load_watermark(&db, mailbox).await.unwrap(),
            third.next_cursor
        );
        assert_eq!(
            load_watermark(&db, Uuid::new_v4())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
        assert!(read_after(&db, mailbox, initial.as_str(), 0).await.is_err());
        assert!(
            read_after(&db, mailbox, initial.as_str(), 101)
                .await
                .is_err()
        );
        let mut foreign = Cursor::decode(initial.as_str(), mailbox).unwrap();
        foreign.mailbox = Uuid::new_v4();
        assert!(
            read_after(&db, mailbox, foreign.encode().as_str(), 1)
                .await
                .is_err()
        );
        foreign.mailbox = mailbox;
        foreign.generation = Uuid::new_v4();
        assert_eq!(
            read_after(&db, mailbox, foreign.encode().as_str(), 1)
                .await
                .unwrap()
                .reset,
            Some(ResetReason::GenerationChanged)
        );
        foreign = Cursor::decode(initial.as_str(), mailbox).unwrap();
        foreign.position = i64::MAX;
        assert_eq!(
            read_after(&db, mailbox, foreign.encode().as_str(), 1)
                .await
                .unwrap()
                .reset,
            Some(ResetReason::FuturePosition)
        );
        // Simulate a damaged ledger without deleting any rows.
        sqlx::query("UPDATE mail_arrival_watermarks SET position=4 WHERE mailbox_id=$1")
            .bind(mailbox)
            .execute(&db)
            .await
            .unwrap();
        let gap = read_after(&db, mailbox, third.next_cursor.as_str(), 1)
            .await
            .unwrap();
        assert_eq!(gap.reset, Some(ResetReason::ReplayGap));
        assert!(gap.arrivals.is_empty());
    }

    #[test]
    fn cursor_validation() {
        let mailbox = Uuid::new_v4();
        let mut cursor = Cursor {
            version: 1,
            generation: Uuid::new_v4(),
            mailbox,
            position: 0,
        };
        let token = cursor.encode();
        assert_eq!(Cursor::decode(token.as_str(), mailbox).unwrap().position, 0);
        assert!(Cursor::decode(token.as_str(), Uuid::new_v4()).is_err());
        for token in ["", "not a cursor", &"a".repeat(513)] {
            assert!(Cursor::decode(token, mailbox).is_err());
        }
        cursor.position = -1;
        assert!(Cursor::decode(cursor.encode().as_str(), mailbox).is_err());
        cursor.position = i64::MAX;
        assert_eq!(
            Cursor::decode(cursor.encode().as_str(), mailbox)
                .unwrap()
                .position,
            i64::MAX
        );
        cursor.version = 2;
        assert!(Cursor::decode(cursor.encode().as_str(), mailbox).is_err());
    }
}
