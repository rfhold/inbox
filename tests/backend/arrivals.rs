//! Opt-in exclusive isolated stack test; never use personal .env or live mail.
use inbox_server::{
    arrivals::{load_watermark, read_after},
    mutation::Command,
    pb::*,
    pipeline::Pipeline,
    service::MailService,
};
use prost::Message as _;
use std::{env, sync::Arc, time::Duration};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires exclusive isolated INBOX_TEST_DATABASE_URL, INBOX_TEST_NATS_URL and INBOX_TEST_S3_*"]
async fn only_new_inbox_delivery_is_an_arrival() {
    let db = sqlx::PgPool::connect(&env::var("INBOX_TEST_DATABASE_URL").unwrap())
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
    let pipeline = Pipeline::connect(&env::var("INBOX_TEST_NATS_URL").unwrap())
        .await
        .unwrap();
    let service = MailService {
        auth: Default::default(),
        principal: None,
        db: db.clone(),
        bucket: Arc::new(*bucket),
        domain: "inbox.test".into(),
        pipeline: Some(pipeline.clone()),
    };
    pipeline.start(service.clone()).await.unwrap();
    service.bootstrap_domains().await.unwrap();
    let mut mailboxes = Vec::new();
    for _ in 0..2 {
        let bytes = pipeline
            .execute(
                Command::Create(
                    CreateMailboxRequest {
                        domain_id: String::new(),
                        local_part: format!("arrival-{}", Uuid::new_v4()),
                        name: "Synthetic arrival test".into(),
                        request_id: Uuid::new_v4().to_string(),
                    }
                    .encode_to_vec(),
                ),
                String::new(),
            )
            .await
            .unwrap();
        mailboxes.push(
            CreateMailboxResponse::decode(bytes.as_slice())
                .unwrap()
                .mailbox
                .unwrap(),
        );
    }
    let sender = Uuid::parse_str(&mailboxes[0].id).unwrap();
    let recipient = Uuid::parse_str(&mailboxes[1].id).unwrap();
    let sent_cursor = load_watermark(&db, sender).await.unwrap();
    let received_cursor = load_watermark(&db, recipient).await.unwrap();
    let request = SubmitMessageRequest {
        from_mailbox_id: sender.to_string(),
        to: vec![mailboxes[1].address.clone()],
        subject: "Synthetic".into(),
        text_body: "Synthetic fixture".into(),
        idempotency_key: Uuid::new_v4().to_string(),
        ..Default::default()
    }
    .encode_to_vec();
    let bytes = pipeline
        .execute(Command::Submit(request.clone()), String::new())
        .await
        .unwrap();
    let submission = SubmitMessageResponse::decode(bytes.as_slice())
        .unwrap()
        .submission
        .unwrap();
    let page = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let page = read_after(&db, recipient, received_cursor.as_str(), 100)
                .await
                .unwrap();
            if !page.arrivals.is_empty() {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(page.arrivals.len(), 1);
    let entry = page.arrivals[0].entry_id;
    assert!(
        read_after(&db, sender, sent_cursor.as_str(), 100)
            .await
            .unwrap()
            .arrivals
            .is_empty(),
        "Sent is not an arrival"
    );
    pipeline
        .execute(
            Command::Read(
                SetReadStateRequest {
                    mailbox_id: recipient.to_string(),
                    entry_id: entry.to_string(),
                    is_read: true,
                    request_id: Uuid::new_v4().to_string(),
                }
                .encode_to_vec(),
            ),
            String::new(),
        )
        .await
        .unwrap();
    pipeline
        .execute(Command::Submit(request), String::new())
        .await
        .unwrap();
    let job: Uuid =
        sqlx::query_scalar("SELECT id FROM deliveries WHERE submission_id=$1 AND mailbox_id=$2")
            .bind(Uuid::parse_str(&submission.id).unwrap())
            .bind(recipient)
            .fetch_one(&db)
            .await
            .unwrap();
    pipeline
        .execute(Command::Deliver(job), String::new())
        .await
        .unwrap();
    // A legacy/reconciled pending job can already have its Inbox entry.
    sqlx::query("UPDATE deliveries SET status=1,entry_id=NULL WHERE id=$1")
        .bind(job)
        .execute(&db)
        .await
        .unwrap();
    pipeline
        .execute(Command::Deliver(job), String::new())
        .await
        .unwrap();
    let reconciled: Uuid = sqlx::query_scalar("SELECT entry_id FROM deliveries WHERE id=$1")
        .bind(job)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(
        reconciled, entry,
        "reconciliation must reuse the Inbox entry"
    );
    assert!(
        read_after(&db, recipient, page.next_cursor.as_str(), 100)
            .await
            .unwrap()
            .arrivals
            .is_empty(),
        "read state, duplicate submission and repeated delivery must not emit arrivals"
    );
    assert_eq!(
        load_watermark(&db, recipient).await.unwrap(),
        page.next_cursor,
        "fresh watches start after all historical arrivals"
    );
}
