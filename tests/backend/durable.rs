//! Opt-in: uses existing isolated PostgreSQL and S3 bucket; never deletes data.
#[path = "nats_isolation.rs"]
mod nats_isolation;
use futures_util::FutureExt;
use inbox_server::{mutation::Command, pipeline::Pipeline};
use inbox_server::{
    pb::{mail_service_server::MailService as _, *},
    service::MailService,
};
use prost::Message as _;
use std::{env, sync::Arc};
use tokio_stream::StreamExt;
use tonic::{Code, Request};
use uuid::Uuid;

async fn delivered(s: &MailService, id: Uuid) {
    for _ in 0..200 {
        if s.submission(id).await.unwrap().status != 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("durable delivery did not complete");
}

async fn staging_capacity(s: &MailService, pipeline: &Pipeline) {
    let client = async_nats::connect(env::var("INBOX_TEST_NATS_URL").unwrap())
        .await
        .unwrap();
    let js = async_nats::jetstream::new(client);
    let mut commands = js.get_stream("INBOX_COMMANDS").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            let quiet:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM deliveries WHERE status=1) AND NOT EXISTS(SELECT 1 FROM command_outbox WHERE NOT published)").fetch_one(&s.db).await.unwrap();
            if quiet && commands.info().await.unwrap().state.messages==0 {break}
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.expect("capacity probe requires quiescent exclusive services");
    let mut stream = js.get_stream("OBJ_INBOX_STAGING").await.unwrap();
    let info = stream.info().await.unwrap();
    let original = info.config.clone();
    let bytes = info.state.bytes;
    let mut limited = original.clone();
    limited.max_bytes = i64::try_from(bytes).unwrap() + 1;
    let local = format!("capacity-{}", Uuid::new_v4());
    if let Err(error) = js.update_stream(limited).await {
        js.update_stream(original.clone())
            .await
            .expect("must restore staging configuration after an uncertain limit update");
        panic!("non-evicting capacity reduction unsupported: {error}");
    }
    let attempted = std::panic::AssertUnwindSafe(tokio::time::timeout(
        std::time::Duration::from_secs(10),
        pipeline.execute(
            Command::Create(
                CreateMailboxRequest {
                    domain_id: String::new(),
                    local_part: local.clone(),
                    name: "Synthetic staging capacity probe".into(),
                    request_id: Uuid::new_v4().to_string(),
                }
                .encode_to_vec(),
            ),
            String::new(),
        ),
    ))
    .catch_unwind()
    .await;
    // Restore before interpreting timeout, failure, or a caught assertion panic.
    let restored = js.update_stream(original.clone()).await;
    assert!(
        restored.is_ok(),
        "must restore the staging limit before any further test work"
    );
    let attempted = match attempted {
        Ok(outcome) => outcome,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    let error = attempted
        .expect("full staging admission must fail promptly")
        .expect_err("full staging must not report success");
    assert_eq!(error.code(), Code::Unavailable);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM mailboxes WHERE local_part=$1)"
        )
        .bind(local)
        .fetch_one(&s.db)
        .await
        .unwrap(),
        "staging exhaustion must not commit a mailbox"
    );
    let restored = stream.info().await.unwrap();
    assert_eq!(restored.config.max_bytes, original.max_bytes);
    assert_eq!(
        restored.state.bytes, bytes,
        "capacity probing must not evict or add staged records"
    );
}

async fn legacy_reconciliation(s: &MailService, original: &Submission) {
    let baseline: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM command_journal),(SELECT count(*) FROM command_outbox)",
    )
    .fetch_one(&s.db)
    .await
    .unwrap();
    let legacy = Uuid::new_v4();
    let pending = Uuid::new_v4();
    let exhausted = Uuid::new_v4();
    let completed_job = Uuid::new_v4();
    let pending_job = Uuid::new_v4();
    let exhausted_job = Uuid::new_v4();
    let completed_entry = Uuid::parse_str(&original.recipients[0].entry_id).unwrap();
    let completed_before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM mailbox_entries e WHERE id=$1")
            .bind(completed_entry)
            .fetch_one(&s.db)
            .await
            .unwrap();
    let message = Uuid::parse_str(&original.message_id).unwrap();
    let (domain_id, domain): (Uuid, String) = sqlx::query_as(
        "SELECT d.id,d.name FROM mailboxes m JOIN domains d ON d.id=m.domain_id WHERE m.id=$1",
    )
    .bind(Uuid::parse_str(&original.from_mailbox_id).unwrap())
    .fetch_one(&s.db)
    .await
    .unwrap();
    let mut tx = s.db.begin().await.unwrap();
    for mailbox in [pending, exhausted] {
        sqlx::query("INSERT INTO mailboxes(id,local_part,name,domain_id,owner_id) VALUES($1,$2,'Legacy reconciliation fixture',$3,'00000000-0000-4000-8000-000000000001')").bind(mailbox).bind(format!("legacy-{mailbox}")).bind(domain_id).execute(&mut *tx).await.unwrap();
    }
    sqlx::query("INSERT INTO submissions(id,message_id,from_mailbox_id,idempotency_key,payload_hash) VALUES($1,$2,$3,$4,'legacy-fixture')").bind(legacy).bind(message).bind(Uuid::parse_str(&original.from_mailbox_id).unwrap()).bind(Uuid::new_v4().to_string()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO deliveries(id,submission_id,mailbox_id,address,entry_id,status,attempts) VALUES($1,$2,$3,$4,$5,2,1)").bind(completed_job).bind(legacy).bind(Uuid::parse_str(&original.recipients[0].mailbox_id).unwrap()).bind(&original.recipients[0].address).bind(completed_entry).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO deliveries(id,submission_id,mailbox_id,address,status,attempts,lease_until) VALUES($1,$2,$3,$4,1,1,now()+interval '5 seconds'),($5,$2,$6,$7,1,8,now()-interval '1 second')").bind(pending_job).bind(legacy).bind(pending).bind(format!("legacy-{pending}@{domain}")).bind(exhausted_job).bind(exhausted).bind(format!("legacy-{exhausted}@{domain}")).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let unmapped: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS(SELECT 1 FROM delivery_commands WHERE delivery_id=ANY($1))",
    )
    .bind(vec![completed_job, pending_job, exhausted_job])
    .fetch_one(&s.db)
    .await
    .unwrap();
    assert!(
        unmapped,
        "legacy jobs must start without command mappings or intents"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT (SELECT count(*) FROM command_journal),(SELECT count(*) FROM command_outbox)"
        )
        .fetch_one(&s.db)
        .await
        .unwrap(),
        baseline,
        "seeding legacy domain rows must not create journal/outbox records"
    );
    let pipeline = Pipeline::connect(&env::var("INBOX_TEST_NATS_URL").unwrap())
        .await
        .unwrap();
    let restarted = MailService {
        pipeline: Some(pipeline.clone()),
        ..s.clone()
    };
    pipeline.start(restarted.clone()).await.unwrap();
    let mapped = async {
        loop {
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM delivery_commands c JOIN command_outbox o ON (CASE WHEN o.subject='INBOX.commands.delivery' THEN convert_from(o.payload,'UTF8')::jsonb->>'id' END)=c.command_id::text WHERE c.delivery_id=ANY($1) AND o.published").bind(vec![pending_job,exhausted_job]).fetch_one(&s.db).await.unwrap();
            if count == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), mapped)
        .await
        .expect("reconciliation must publish both legacy commands");
    let lease_guarded:bool=sqlx::query_scalar("SELECT status=1 AND attempts=1 AND entry_id IS NULL AND lease_until>now() FROM deliveries WHERE id=$1").bind(pending_job).fetch_one(&s.db).await.unwrap();
    assert!(
        lease_guarded,
        "reconciliation must preserve the active legacy lease"
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM mailbox_entries WHERE mailbox_id=$1 AND message_id=$2 AND folder=1").bind(pending).bind(message).fetch_one(&s.db).await.unwrap(),0);
    tokio::time::timeout(std::time::Duration::from_secs(6),async {loop {
        let (active,untouched):(bool,bool)=sqlx::query_as("SELECT COALESCE(lease_until>now(),false),status=1 AND attempts=1 AND entry_id IS NULL FROM deliveries WHERE id=$1").bind(pending_job).fetch_one(&s.db).await.unwrap();
        if !active {break}
        assert!(untouched,"a future legacy lease must not be claimed early");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }}).await.expect("fixture lease must expire within its bounded window");
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if restarted.submission(legacy).await.unwrap().status != 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("legacy jobs must finish after lease expiry");
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM mailbox_entries WHERE mailbox_id=$1 AND message_id=$2 AND folder=1").bind(pending).bind(message).fetch_one(&s.db).await.unwrap(),1);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT attempts FROM deliveries WHERE id=$1 AND status=2")
            .bind(pending_job)
            .fetch_one(&s.db)
            .await
            .unwrap(),
        2
    );
    assert_eq!(restarted.submission(legacy).await.unwrap().status, 3);
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT entry_id FROM deliveries WHERE id=$1 AND status=2 AND attempts=1"
        )
        .bind(completed_job)
        .fetch_one(&s.db)
        .await
        .unwrap(),
        completed_entry
    );
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT to_jsonb(e) FROM mailbox_entries e WHERE id=$1"
        )
        .bind(completed_entry)
        .fetch_one(&s.db)
        .await
        .unwrap(),
        completed_before,
        "reconciliation must not modify the completed recipient's existing entry"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>(
            "SELECT attempts FROM deliveries WHERE id=$1 AND status=3 AND entry_id IS NULL"
        )
        .bind(exhausted_job)
        .fetch_one(&s.db)
        .await
        .unwrap(),
        8
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM mailbox_entries WHERE mailbox_id=$1 AND message_id=$2"
        )
        .bind(exhausted)
        .bind(message)
        .fetch_one(&s.db)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM delivery_commands WHERE delivery_id=$1")
            .bind(completed_job)
            .fetch_one(&s.db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM command_journal j JOIN delivery_commands c ON c.command_id=j.id WHERE c.delivery_id=ANY($1)").bind(vec![pending_job,exhausted_job]).fetch_one(&s.db).await.unwrap(),2);
    pipeline.shutdown().await;
}

#[tokio::test]
#[ignore = "requires INBOX_TEST_DATABASE_URL, INBOX_TEST_NATS_URL and INBOX_TEST_S3_* (isolated services)"]
async fn durable_end_to_end() {
    let db = sqlx::PgPool::connect(&env::var("INBOX_TEST_DATABASE_URL").expect("test database"))
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&db).await.unwrap();
    let credentials = s3::creds::Credentials::new(
        Some(&env::var("INBOX_TEST_S3_ACCESS_KEY").unwrap()),
        Some(&env::var("INBOX_TEST_S3_SECRET_KEY").unwrap()),
        None,
        None,
        None,
    )
    .unwrap();
    let bucket = s3::Bucket::new(
        &env::var("INBOX_TEST_S3_BUCKET").unwrap(),
        s3::Region::Custom {
            region: "garage".into(),
            endpoint: env::var("INBOX_TEST_S3_ENDPOINT").unwrap(),
        },
        credentials,
    )
    .unwrap()
    .with_path_style();
    let pipeline = Pipeline::connect(&env::var("INBOX_TEST_NATS_URL").expect("test NATS"))
        .await
        .unwrap();
    let s = MailService {
        auth: Default::default(),
        principal: None,
        db: db.clone(),
        bucket: Arc::new(*bucket),
        domain: "inbox.test".into(),
        pipeline: Some(pipeline.clone()),
    };
    pipeline.start(s.clone()).await.unwrap();
    s.bootstrap_domains().await.unwrap();
    let observer = async_nats::connect(env::var("INBOX_TEST_NATS_URL").unwrap())
        .await
        .unwrap();
    let mut quiet = false;
    for _ in 0..200 {
        quiet=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM deliveries WHERE status=1) AND NOT EXISTS(SELECT 1 FROM command_outbox WHERE NOT published)").fetch_one(&db).await.unwrap();
        if quiet {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(quiet, "idle-rate checks require drained application work");
    let mut pulls = observer
        .subscribe("$JS.API.CONSUMER.MSG.NEXT.INBOX_COMMANDS.INBOX_MUTATIONS")
        .await
        .unwrap();
    observer.flush().await.unwrap();
    let mut idle_pulls = 0;
    let _ = tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while pulls.next().await.is_some() {
            idle_pulls += 1;
            assert!(
                idle_pulls <= 1,
                "empty mutation queues must use waiting pulls"
            )
        }
    })
    .await;
    drop(pulls);
    let mut changes = s
        .watch_changes(Request::new(WatchChangesRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let reset = changes.next().await.unwrap().unwrap();
    assert_eq!(reset.kind, 1);
    let idle_since = tokio::time::Instant::now();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), changes.next())
            .await
            .is_err(),
        "an idle watch must not emit a heartbeat per broker round trip"
    );
    let heartbeat = tokio::time::timeout(std::time::Duration::from_secs(18), changes.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(heartbeat.kind, 5);
    assert_eq!(heartbeat.resume_token, reset.resume_token);
    assert!(idle_since.elapsed() >= std::time::Duration::from_secs(14));
    let mut resumed = s
        .watch_changes(Request::new(WatchChangesRequest {
            resume_token: reset.resume_token.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    let ready = tokio::time::timeout(std::time::Duration::from_secs(1), resumed.next())
        .await
        .expect("a valid resumed subscription must signal readiness immediately")
        .unwrap()
        .unwrap();
    assert_eq!(ready.kind, ChangeKind::Heartbeat as i32);
    assert_eq!(
        ready.resume_token, reset.resume_token,
        "readiness must not advance the resumed cursor"
    );
    assert!(
        ready.mailbox_id.is_empty() && ready.entry_id.is_empty() && ready.submission_id.is_empty(),
        "readiness must not invalidate any identifiers"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), resumed.next())
            .await
            .is_err(),
        "resumed readiness must not restart idle heartbeat flooding"
    );
    drop(resumed);
    let local = format!("test-{}", Uuid::new_v4());
    let a = s
        .create_mailbox(Request::new(CreateMailboxRequest {
            domain_id: String::new(),
            local_part: local.clone(),
            name: "Integration".into(),
            request_id: String::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .mailbox
        .unwrap();
    let mailbox_change = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = changes.next().await.unwrap().unwrap();
            if event.kind == 2 && event.mailbox_id == a.id {
                break event;
            }
        }
    })
    .await
    .unwrap();
    assert!(!mailbox_change.resume_token.is_empty());
    drop(changes);
    let mut replay = s
        .watch_changes(Request::new(WatchChangesRequest {
            resume_token: reset.resume_token,
        }))
        .await
        .unwrap()
        .into_inner();
    let ready = tokio::time::timeout(std::time::Duration::from_secs(1), replay.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ready.kind, ChangeKind::Heartbeat as i32);
    let replayed = tokio::time::timeout(std::time::Duration::from_secs(5), replay.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(replayed.mailbox_id, a.id);
    assert_eq!(replayed.resume_token, mailbox_change.resume_token);
    drop(replay);

    let request_id = Uuid::new_v4().to_string();
    let gated = CreateMailboxRequest {
        domain_id: String::new(),
        local_part: format!("gate-{}", Uuid::new_v4()),
        name: "Queued is not committed".into(),
        request_id: request_id.clone(),
    };
    let actor = s.principal(&Request::new(gated.clone())).unwrap().actor;
    let request_scope = Command::authorized(actor, Command::Create(gated.encode_to_vec()))
        .request_scope()
        .unwrap()
        .unwrap();
    let mut lock = db.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1,0))")
        .bind(&request_scope)
        .execute(&mut *lock)
        .await
        .unwrap();
    let called = s.clone();
    let payload = gated.clone();
    let pending = tokio::spawn(async move { called.create_mailbox(Request::new(payload)).await });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!pending.is_finished(), "PubAck must not complete the RPC");
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM mailboxes WHERE local_part=$1)"
        )
        .bind(&gated.local_part)
        .fetch_one(&db)
        .await
        .unwrap()
    );
    sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1,0))")
        .bind(&request_scope)
        .execute(&mut *lock)
        .await
        .unwrap();
    let committed = pending
        .await
        .unwrap()
        .unwrap()
        .into_inner()
        .mailbox
        .unwrap();
    let (one, two) = tokio::join!(
        s.create_mailbox(Request::new(gated.clone())),
        s.create_mailbox(Request::new(gated.clone()))
    );
    assert_eq!(one.unwrap().into_inner().mailbox.unwrap().id, committed.id);
    assert_eq!(two.unwrap().into_inner().mailbox.unwrap().id, committed.id);
    let mut conflict = gated;
    conflict.name = "changed".into();
    assert_eq!(
        s.create_mailbox(Request::new(conflict))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );

    // Hold only the outbox publisher: committed state and confirmation intent
    // must survive independently of successful outbox publication.
    sqlx::query("SELECT pg_advisory_lock(763194815)")
        .execute(&mut *lock)
        .await
        .unwrap();
    let nats = async_nats::connect(env::var("INBOX_TEST_NATS_URL").unwrap())
        .await
        .unwrap();
    let js = async_nats::jetstream::new(nats);
    let command_id = Uuid::new_v4();
    let lost = CreateMailboxRequest {
        domain_id: String::new(),
        local_part: format!("replay-{}", Uuid::new_v4()),
        name: "Lost caller confirmation".into(),
        request_id: String::new(),
    };
    let envelope = inbox_server::pipeline::Envelope::inline(
        command_id,
        Command::Create(lost.encode_to_vec()),
        "",
    )
    .unwrap();
    let bytes = envelope.encode().unwrap();
    js.publish("INBOX.commands.api", bytes.clone().into())
        .await
        .unwrap()
        .await
        .unwrap();
    for _ in 0..100 {
        if sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM command_journal WHERE id=$1)")
            .bind(command_id)
            .fetch_one(&db)
            .await
            .unwrap()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let intent: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM command_outbox WHERE subject=$1 AND NOT published)",
    )
    .bind(format!("INBOX.results.{command_id}"))
    .fetch_one(&db)
    .await
    .unwrap();
    assert!(intent, "commit must atomically retain confirmation intent");
    sqlx::query("SELECT pg_advisory_unlock(763194815)")
        .execute(&mut *lock)
        .await
        .unwrap();
    let results = js.get_stream("INBOX_RESULTS").await.unwrap();
    for _ in 0..100 {
        if results
            .get_last_raw_message_by_subject(&format!("INBOX.results.{command_id}"))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        results
            .get_last_raw_message_by_subject(&format!("INBOX.results.{command_id}"))
            .await
            .is_ok()
    );
    // No Nats-Msg-Id: exercise replay beyond broker deduplication guarantees.
    js.publish("INBOX.commands.api", bytes.into())
        .await
        .unwrap()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM mailboxes WHERE local_part=$1")
            .bind(lost.local_part)
            .fetch_one(&db)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        s.create_mailbox(Request::new(CreateMailboxRequest {
            domain_id: String::new(),
            local_part: local.to_uppercase(),
            name: "duplicate".into(),
            request_id: String::new(),
        }))
        .await
        .unwrap_err()
        .code(),
        Code::AlreadyExists
    );
    let mut r = SubmitMessageRequest {
        from_mailbox_id: a.id.clone(),
        to: vec![a.address.clone()],
        cc: vec![a.address.clone()],
        subject: "Durable test".into(),
        text_body: "plain body".into(),
        idempotency_key: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    let mut bad = r.clone();
    bad.to.push(format!("unknown@{}", a.domain));
    assert_eq!(
        s.submit_message(Request::new(bad))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let (first, retry) = tokio::join!(
        s.submit_message(Request::new(r.clone())),
        s.submit_message(Request::new(r.clone()))
    );
    let first = first.unwrap().into_inner().submission.unwrap();
    let retry = retry.unwrap().into_inner().submission.unwrap();
    assert_eq!(first.id, retry.id);
    assert_eq!(first.recipients.len(), 1);
    r.subject = "conflicting".into();
    assert_eq!(
        s.submit_message(Request::new(r)).await.unwrap_err().code(),
        Code::AlreadyExists
    );
    delivered(&s, Uuid::parse_str(&first.id).unwrap()).await;
    let delivery = s
        .get_submission(Request::new(GetSubmissionRequest {
            submission_id: first.id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .submission
        .unwrap();
    assert_eq!(delivery.status, 2);
    // Replay a committed job after a simulated lost acknowledgement.
    sqlx::query("UPDATE deliveries SET status=1,lease_until=now()-interval '1 second' WHERE submission_id=$1").bind(Uuid::parse_str(&first.id).unwrap()).execute(&db).await.unwrap();
    let job: Uuid = sqlx::query_scalar("SELECT id FROM deliveries WHERE submission_id=$1")
        .bind(Uuid::parse_str(&first.id).unwrap())
        .fetch_one(&db)
        .await
        .unwrap();
    pipeline
        .execute(Command::Deliver(job), String::new())
        .await
        .unwrap();
    let list = s
        .list_messages(Request::new(ListMessagesRequest {
            mailbox_id: a.id.clone(),
            folder: 1,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.entries.len(), 1);
    let e = &list.entries[0];
    assert!(!e.is_read);
    let get = GetMessageRequest {
        mailbox_id: a.id.clone(),
        entry_id: e.id.clone(),
    };
    let message = s
        .get_message(Request::new(get))
        .await
        .unwrap()
        .into_inner()
        .message
        .unwrap();
    assert_eq!(message.text_body.trim(), "plain body");
    assert_eq!(message.reply_to_address, a.address);
    let raw = s
        .get_raw_message(Request::new(GetRawMessageRequest {
            mailbox_id: a.id.clone(),
            entry_id: e.id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .raw_message;
    assert!(!raw.is_empty());
    let read_request = SetReadStateRequest {
        mailbox_id: a.id.clone(),
        entry_id: e.id.clone(),
        is_read: true,
        request_id: Uuid::new_v4().to_string(),
    };
    let read = s
        .set_read_state(Request::new(read_request.clone()))
        .await
        .unwrap()
        .into_inner()
        .entry
        .unwrap();
    assert!(read.is_read);
    assert_eq!(
        s.set_read_state(Request::new(read_request.clone()))
            .await
            .unwrap()
            .into_inner()
            .entry
            .unwrap(),
        read
    );
    let mut conflict = read_request;
    conflict.is_read = false;
    assert_eq!(
        s.set_read_state(Request::new(conflict))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let smtp=b"From: external@example.org\r\nTo: local@inbox.test\r\nReply-To: preferred@example.org\r\nMessage-ID: <source@example.org>\r\nSubject: Raw\r\n\r\nuntouched \x80 bytes\r\n".to_vec();
    let sid = s
        .accept_smtp(
            smtp.clone(),
            vec![(Uuid::parse_str(&a.id).unwrap(), a.address.clone())],
        )
        .await
        .unwrap();
    delivered(&s, sid).await;
    let sub = s.submission(sid).await.unwrap();
    assert!(
        sub.recipients[0].entry_id.is_empty(),
        "internal SMTP submissions have no sender-owner projection"
    );
    let smtp_entry=sqlx::query_scalar::<_,Uuid>("SELECT e.id FROM mailbox_entries e JOIN submissions s ON s.message_id=e.message_id WHERE s.id=$1 AND e.mailbox_id=$2 AND e.folder=1").bind(sid).bind(Uuid::parse_str(&a.id).unwrap()).fetch_one(&s.db).await.unwrap().to_string();
    let received = s
        .get_raw_message(Request::new(GetRawMessageRequest {
            mailbox_id: a.id.clone(),
            entry_id: smtp_entry.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(received.raw_message, smtp);
    let reply = SubmitMessageRequest {
        from_mailbox_id: a.id.clone(),
        to: vec![a.address.clone()],
        subject: "Re: Raw".into(),
        text_body: "reply".into(),
        reply_to_entry_id: Some(smtp_entry),
        source_mailbox_id: Some(a.id.clone()),
        idempotency_key: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    let sub = s
        .submit_message(Request::new(reply))
        .await
        .unwrap()
        .into_inner()
        .submission
        .unwrap();
    delivered(&s, Uuid::parse_str(&sub.id).unwrap()).await;
    let sub = s
        .submission(Uuid::parse_str(&sub.id).unwrap())
        .await
        .unwrap();
    let reply = s
        .get_message(Request::new(GetMessageRequest {
            mailbox_id: a.id.clone(),
            entry_id: sub.recipients[0].entry_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .message
        .unwrap();
    assert_eq!(reply.in_reply_to, "source@example.org");
    assert!(reply.references.contains(&"source@example.org".to_string()));
    sqlx::query("UPDATE mailbox_entries SET created_at='2026-01-01T00:00:00Z' WHERE mailbox_id=$1")
        .bind(Uuid::parse_str(&a.id).unwrap())
        .execute(&db)
        .await
        .unwrap();
    let mut token = String::new();
    let mut entries = Vec::new();
    loop {
        let page = s
            .list_messages(Request::new(ListMessagesRequest {
                mailbox_id: a.id.clone(),
                folder: 1,
                page_size: 1,
                page_token: token,
            }))
            .await
            .unwrap()
            .into_inner();
        entries.extend(page.entries.iter().map(|e| e.id.clone()));
        token = page.next_page_token;
        if token.is_empty() {
            break;
        }
        assert_eq!(
            s.list_messages(Request::new(ListMessagesRequest {
                mailbox_id: a.id.clone(),
                folder: 2,
                page_size: 1,
                page_token: token.clone()
            }))
            .await
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );
    }
    assert_eq!(entries.len(), 3);
    let unique: std::collections::BTreeSet<_> = entries.iter().collect();
    assert_eq!(unique.len(), 3);
    assert!(entries.windows(2).all(|w| w[0] > w[1]));
    let mut large =
        b"From: source@example.org\r\nTo: local@inbox.test\r\nSubject: Staged raw limit\r\n\r\n"
            .to_vec();
    large.resize(25 * 1024 * 1024 - 2, b'x');
    large.extend_from_slice(b"\r\n");
    let large_id = s
        .accept_smtp(
            large.clone(),
            vec![(Uuid::parse_str(&a.id).unwrap(), a.address.clone())],
        )
        .await
        .unwrap();
    delivered(&s, large_id).await;
    let large_sub = s.submission(large_id).await.unwrap();
    assert!(large_sub.recipients[0].entry_id.is_empty());
    let large_entry=sqlx::query_scalar::<_,Uuid>("SELECT e.id FROM mailbox_entries e JOIN submissions s ON s.message_id=e.message_id WHERE s.id=$1 AND e.mailbox_id=$2 AND e.folder=1").bind(large_id).bind(Uuid::parse_str(&a.id).unwrap()).fetch_one(&s.db).await.unwrap().to_string();
    let archived = s
        .get_raw_message(Request::new(GetRawMessageRequest {
            mailbox_id: a.id.clone(),
            entry_id: large_entry,
        }))
        .await
        .unwrap()
        .into_inner()
        .raw_message;
    assert_eq!(archived, large);

    assert_eq!(
        s.watch_changes(Request::new(WatchChangesRequest {
            resume_token: "invalid-history".into()
        }))
        .await
        .err()
        .unwrap()
        .code(),
        Code::InvalidArgument,
        "malformed external cursors must fail before broker history lookup"
    );
    let principal = s.principal(&Request::new(())).unwrap();
    let history = s.auth.bind_cursor(
        pipeline.namespace(),
        &principal.actor,
        "changes",
        "invalid-history",
    );
    let mut invalid = s
        .watch_changes(Request::new(WatchChangesRequest {
            resume_token: history,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(invalid.next().await.unwrap().unwrap().kind, 1);
    drop(invalid);
    let mut slow = s
        .watch_changes(Request::new(WatchChangesRequest::default()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(slow.next().await.unwrap().unwrap().kind, 1);
    for _ in 0..64 {
        js.publish(
            "INBOX.changes",
            ChangeEvent {
                kind: 2,
                mailbox_id: Uuid::new_v4().to_string(),
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_secs(17)).await;
    let mut buffered = 0;
    let mut terminal = None;
    while let Some(event) = tokio::time::timeout(std::time::Duration::from_secs(2), slow.next())
        .await
        .unwrap()
    {
        terminal = Some(event.unwrap().kind);
        buffered += 1;
        assert!(
            buffered <= 17,
            "slow clients must not accumulate unbounded events"
        )
    }
    assert_eq!(
        terminal,
        Some(ChangeKind::Reset as i32),
        "slow clients must receive a terminal reset before disconnect"
    );
    drop(slow);
    let failed = s
        .accept_smtp(
            smtp,
            vec![(Uuid::parse_str(&a.id).unwrap(), a.address.clone())],
        )
        .await
        .unwrap();
    delivered(&s, failed).await;
    sqlx::query("UPDATE deliveries SET status=1,attempts=8,lease_until=now()-interval '1 second' WHERE submission_id=$1").bind(failed).execute(&db).await.unwrap();
    let job: Uuid = sqlx::query_scalar("SELECT id FROM deliveries WHERE submission_id=$1")
        .bind(failed)
        .fetch_one(&db)
        .await
        .unwrap();
    pipeline
        .execute(Command::Deliver(job), String::new())
        .await
        .unwrap();
    assert_eq!(s.submission(failed).await.unwrap().status, 4);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let credentials = s3::creds::Credentials::new(
        Some(&env::var("INBOX_TEST_S3_ACCESS_KEY").unwrap()),
        Some(&env::var("INBOX_TEST_S3_SECRET_KEY").unwrap()),
        None,
        None,
        None,
    )
    .unwrap();
    let unavailable_bucket = s3::Bucket::new(
        &env::var("INBOX_TEST_S3_BUCKET").unwrap(),
        s3::Region::Custom {
            region: "garage".into(),
            endpoint,
        },
        credentials,
    )
    .unwrap()
    .with_path_style()
    .with_request_timeout(std::time::Duration::from_millis(200))
    .unwrap();
    staging_capacity(&s, &pipeline).await;
    pipeline.shutdown().await;
    legacy_reconciliation(&s, &delivery).await;
    let failing_pipeline = Pipeline::connect(&env::var("INBOX_TEST_NATS_URL").unwrap())
        .await
        .unwrap();
    let unavailable = MailService {
        auth: Default::default(),
        principal: None,
        db: db.clone(),
        bucket: Arc::new(*unavailable_bucket),
        domain: "inbox.test".into(),
        pipeline: Some(failing_pipeline.clone()),
    };
    failing_pipeline.start(unavailable.clone()).await.unwrap();
    let subject = format!("s3-failure-{}", Uuid::new_v4());
    let raw =
        format!("From: sender@example.org\r\nSubject: {subject}\r\n\r\nsynthetic").into_bytes();
    assert_eq!(
        unavailable
            .accept_smtp(
                raw,
                vec![(Uuid::parse_str(&a.id).unwrap(), a.address.clone())]
            )
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM messages WHERE content->>'subject'=$1")
            .bind(subject)
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(count, 0);
    let transient = SubmitMessageRequest {
        from_mailbox_id: a.id.clone(),
        to: vec![a.address],
        subject: "Retry transient archive failure".into(),
        text_body: "synthetic".into(),
        idempotency_key: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    assert_eq!(
        unavailable
            .submit_message(Request::new(transient.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    failing_pipeline.shutdown().await;
    let recovered_pipeline = Pipeline::connect(&env::var("INBOX_TEST_NATS_URL").unwrap())
        .await
        .unwrap();
    let recovered = MailService {
        pipeline: Some(recovered_pipeline.clone()),
        ..s
    };
    recovered_pipeline.start(recovered.clone()).await.unwrap();
    assert!(
        recovered
            .submit_message(Request::new(transient))
            .await
            .unwrap()
            .into_inner()
            .submission
            .is_some()
    );
    recovered_pipeline.shutdown().await;
}
