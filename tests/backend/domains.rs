//! Opt-in, exclusive synthetic stack only; fixtures are retained.
use inbox_server::{
    mutation::Command,
    pb::{mail_service_server::MailService as _, *},
    pipeline::Pipeline,
    service::MailService,
};
use std::{env, sync::Arc};
use tokio_stream::StreamExt;
use tonic::{Code, Request};
use uuid::Uuid;

async fn create(s: &MailService, name: &str, receive: bool, send: bool) -> Domain {
    s.create_domain(Request::new(CreateDomainRequest {
        name: name.into(),
        receive_enabled: receive,
        send_enabled: send,
        request_id: Uuid::new_v4().to_string(),
    }))
    .await
    .unwrap()
    .into_inner()
    .domain
    .unwrap()
}

async fn policy(s: &MailService, d: &Domain, receive: bool, send: bool) {
    s.set_domain_policy(Request::new(SetDomainPolicyRequest {
        domain_id: d.id.clone(),
        receive_enabled: receive,
        send_enabled: send,
        request_id: Uuid::new_v4().to_string(),
    }))
    .await
    .unwrap();
}

async fn mailbox(s: &MailService, domain: &str, local: &str) -> Mailbox {
    s.create_mailbox(Request::new(CreateMailboxRequest {
        domain_id: domain.into(),
        local_part: local.into(),
        name: "Synthetic domain fixture".into(),
        request_id: Uuid::new_v4().to_string(),
    }))
    .await
    .unwrap()
    .into_inner()
    .mailbox
    .unwrap()
}

async fn send(s: &MailService, r: &SubmitMessageRequest) -> Result<Submission, tonic::Status> {
    Ok(s.submit_message(Request::new(r.clone()))
        .await?
        .into_inner()
        .submission
        .unwrap())
}

#[tokio::test]
#[ignore = "requires exclusive isolated INBOX_TEST_DATABASE_URL, INBOX_TEST_NATS_URL and INBOX_TEST_S3_*"]
async fn persisted_domain_policies_and_durable_acceptance() {
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
    let bootstrap: Uuid = sqlx::query_scalar("SELECT domain_id FROM domain_defaults")
        .fetch_one(&db)
        .await
        .unwrap();
    let original_policy: (bool, bool) =
        sqlx::query_as("SELECT receive_enabled,send_enabled FROM domains WHERE id=$1")
            .bind(bootstrap)
            .fetch_one(&db)
            .await
            .unwrap();
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM domains")
        .fetch_one(&db)
        .await
        .unwrap();
    let mut restarted = s.clone();
    restarted.domain = "different-bootstrap.test".into();
    restarted.bootstrap_domains().await.unwrap();
    assert_eq!(
        before,
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domains")
            .fetch_one(&db)
            .await
            .unwrap()
    );
    assert_eq!(
        bootstrap,
        sqlx::query_scalar::<_, Uuid>("SELECT domain_id FROM domain_defaults")
            .fetch_one(&db)
            .await
            .unwrap()
    );

    let mut changes = s
        .watch_changes(Request::new(WatchChangesRequest::default()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        changes.next().await.unwrap().unwrap().kind,
        ChangeKind::Reset as i32
    );
    let a = create(&s, &format!("A-{}.test", Uuid::new_v4()), false, true).await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let event = changes.next().await.unwrap().unwrap();
            if event.kind == ChangeKind::Domain as i32 && event.domain_id == a.id {
                assert!(event.mailbox_id.is_empty());
                assert!(event.entry_id.is_empty());
                assert!(event.submission_id.is_empty());
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(changes);
    let b = create(&s, &format!("b-{}.test", Uuid::new_v4()), true, false).await;
    assert_eq!(a.name, a.name.to_ascii_lowercase());
    let local = format!("user-{}", Uuid::new_v4());
    let sender = mailbox(&s, &a.id, &local).await;
    let receiver = mailbox(&s, &b.id, &local).await;
    assert_ne!(sender.id, receiver.id);
    assert_eq!(sender.address, format!("{local}@{}", a.name));
    let default = mailbox(&restarted, "", &format!("default-{}", Uuid::new_v4())).await;
    assert_eq!(default.domain_id, bootstrap.to_string());
    let bad = s
        .create_mailbox(Request::new(CreateMailboxRequest {
            local_part: local.clone(),
            domain_id: Uuid::new_v4().to_string(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(bad.code(), Code::NotFound);
    let r = SubmitMessageRequest {
        from_mailbox_id: sender.id.clone(),
        to: vec![receiver.address.clone()],
        subject: "Synthetic".into(),
        text_body: "Synthetic".into(),
        idempotency_key: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    let accepted = send(&s, &r).await.unwrap();
    let raw = s
        .get_raw_message(Request::new(GetRawMessageRequest {
            mailbox_id: sender.id.clone(),
            entry_id: sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM mailbox_entries WHERE mailbox_id=$1 AND message_id=$2 AND folder=2",
            )
            .bind(Uuid::parse_str(&sender.id).unwrap())
            .bind(Uuid::parse_str(&accepted.message_id).unwrap())
            .fetch_one(&db)
            .await
            .unwrap()
            .to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .raw_message;
    let parsed = inbox_server::service::parse(&raw).unwrap();
    assert_eq!(parsed.from, sender.address);
    assert!(parsed.internet_id.ends_with(&format!("@{}", a.name)));
    let mut reverse = r.clone();
    reverse.from_mailbox_id = receiver.id.clone();
    reverse.to = vec![receiver.address.clone()];
    assert_eq!(
        send(&s, &reverse).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert!(s.recipient(&receiver.address).await.is_ok());
    assert_eq!(
        s.recipient(&sender.address).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    for bad in [
        format!("missing@{}", b.name),
        "external@unknown.test".into(),
    ] {
        let mut invalid = r.clone();
        invalid.idempotency_key = Uuid::new_v4().to_string();
        invalid.to.push(bad);
        assert_eq!(
            send(&s, &invalid).await.unwrap_err().code(),
            Code::InvalidArgument
        );
        assert!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM submissions WHERE idempotency_key=$1)"
            )
            .bind(&invalid.idempotency_key)
            .fetch_one(&db)
            .await
            .unwrap()
        );
    }
    policy(&s, &a, false, false).await;
    policy(&s, &b, false, false).await;
    assert_eq!(send(&s, &r).await.unwrap().id, accepted.id);
    let mut changed = r.clone();
    changed.text_body.push('x');
    assert_eq!(
        send(&s, &changed).await.unwrap_err().code(),
        Code::AlreadyExists
    );
    let _disabled_mailbox = mailbox(&s, &b.id, &format!("disabled-{}", Uuid::new_v4())).await;
    let mut fresh = r.clone();
    fresh.idempotency_key = Uuid::new_v4().to_string();
    assert_eq!(
        send(&s, &fresh).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    policy(&s, &a, false, true).await;
    assert_eq!(
        send(&s, &fresh).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        s.recipient(&receiver.address).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let raw = b"From: external@example.org\r\nTo: fixture@example.org\r\n\r\nSynthetic".to_vec();
    assert_eq!(
        pipeline
            .execute(
                Command::Smtp {
                    raw: raw.clone(),
                    recipients: vec![receiver.address.clone()]
                },
                String::new()
            )
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    policy(&s, &b, true, false).await;
    assert!(
        pipeline
            .execute(
                Command::Smtp {
                    raw,
                    recipients: vec![receiver.address.clone()]
                },
                String::new()
            )
            .await
            .is_ok()
    );

    let request = CreateDomainRequest {
        name: format!("retry-{}.test", Uuid::new_v4()),
        receive_enabled: true,
        send_enabled: false,
        request_id: Uuid::new_v4().to_string(),
    };
    let first = s
        .create_domain(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        first,
        s.create_domain(Request::new(request.clone()))
            .await
            .unwrap()
            .into_inner()
    );
    let mut conflict = request.clone();
    conflict.send_enabled = true;
    assert_eq!(
        s.create_domain(Request::new(conflict))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    assert_eq!(
        s.create_domain(Request::new(CreateDomainRequest {
            name: "missing-key.test".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
    let mut no_broker = s.clone();
    no_broker.pipeline = None;
    assert_eq!(
        no_broker
            .create_domain(Request::new(request))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    let page = s
        .list_domains(Request::new(ListDomainsRequest {
            page_size: 1,
            page_token: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.domains.len(), 1);
    assert!(!page.next_page_token.is_empty());
    assert_eq!(
        s.list_domains(Request::new(ListDomainsRequest {
            page_size: 2,
            page_token: page.next_page_token
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
    for _ in 0..200 {
        if s.submission(Uuid::parse_str(&accepted.id).unwrap())
            .await
            .unwrap()
            .status
            == 2
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        s.submission(Uuid::parse_str(&accepted.id).unwrap())
            .await
            .unwrap()
            .status,
        2
    );
    // Bootstrap never re-enables persisted policy.
    let initial = Domain {
        id: bootstrap.to_string(),
        ..Default::default()
    };
    policy(&s, &initial, false, false).await;
    pipeline.shutdown().await;
    let pipeline = Pipeline::connect(&env::var("INBOX_TEST_NATS_URL").unwrap())
        .await
        .unwrap();
    restarted.pipeline = Some(pipeline.clone());
    pipeline.start(restarted.clone()).await.unwrap();
    restarted.bootstrap_domains().await.unwrap();
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT receive_enabled OR send_enabled FROM domains WHERE id=$1"
        )
        .bind(bootstrap)
        .fetch_one(&db)
        .await
        .unwrap()
    );
    policy(&restarted, &initial, original_policy.0, original_policy.1).await;
    pipeline.shutdown().await;
}
