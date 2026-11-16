//! Retained synthetic fixtures; never run against a shared or production stack.
use inbox_server::{
    auth::{Actor, LOCAL_USER, Principal},
    mutation::Command,
    pb::{mail_service_server::MailService as _, *},
    pipeline::Pipeline,
    service::MailService,
};
use prost::Message as _;
use std::{env, sync::Arc, time::Duration};
use tonic::{Code, Request};
use uuid::Uuid;

fn as_user(base: &MailService, user_id: Uuid, administrator: bool) -> MailService {
    let mut service = base.clone();
    service.principal = Some(Principal::delegated(
        Actor {
            user_id,
            administrator,
        },
        chrono::Utc::now() + chrono::Duration::hours(1),
    ));
    service
}
async fn mailbox(
    service: &MailService,
    domain: &str,
    local: &str,
    key: &str,
) -> Result<Mailbox, tonic::Status> {
    Ok(service
        .create_mailbox(Request::new(CreateMailboxRequest {
            domain_id: domain.into(),
            local_part: local.into(),
            name: "Private synthetic fixture".into(),
            request_id: key.into(),
        }))
        .await?
        .into_inner()
        .mailbox
        .unwrap())
}
#[tokio::test]
#[ignore = "requires exclusive isolated INBOX_TEST_DATABASE_URL, INBOX_TEST_NATS_URL and INBOX_TEST_S3_*; retains fixtures"]
async fn two_users_cannot_cross_mail_authorization_or_command_results() {
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
    let base = MailService {
        db: db.clone(),
        bucket: Arc::new(*bucket),
        domain: "inbox.test".into(),
        pipeline: Some(pipeline.clone()),
        auth: Default::default(),
        principal: None,
    };
    pipeline.start(base.clone()).await.unwrap();
    base.bootstrap_domains().await.unwrap();
    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();
    for user in [alice, bob] {
        sqlx::query(
            "INSERT INTO inbox_users(id,issuer,subject) VALUES($1,'https://synthetic.example',$2)",
        )
        .bind(user)
        .bind(user.to_string())
        .execute(&db)
        .await
        .unwrap();
    }
    let a = as_user(&base, alice, false);
    let b = as_user(&base, bob, false);
    sqlx::query("UPDATE inbox_users SET administrator=true WHERE id=$1")
        .bind(alice)
        .execute(&db)
        .await
        .unwrap();
    let admin = as_user(&base, alice, true);
    let domain = admin
        .create_domain(Request::new(CreateDomainRequest {
            name: format!("auth-{}.test", Uuid::new_v4()),
            receive_enabled: true,
            send_enabled: true,
            request_id: Uuid::new_v4().to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .domain
        .unwrap();
    let not_admin = CreateDomainRequest {
        name: format!("denied-{}.test", Uuid::new_v4()),
        receive_enabled: true,
        send_enabled: true,
        request_id: Uuid::new_v4().to_string(),
    };
    assert_eq!(
        a.create_domain(Request::new(not_admin.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        pipeline
            .execute(
                Command::authorized(
                    Actor {
                        user_id: alice,
                        administrator: false
                    },
                    Command::CreateDomain(not_admin.encode_to_vec())
                ),
                String::new()
            )
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "consumer enforces administrator authority"
    );
    let shared_key = Uuid::new_v4().to_string();
    let am = mailbox(&a, &domain.id, "alice", &shared_key).await.unwrap();
    let bm = mailbox(&b, &domain.id, "bob", &shared_key).await.unwrap();
    assert_ne!(am.id, bm.id, "same request key is isolated per actor");
    assert_eq!(
        mailbox(&b, &domain.id, "alice", &Uuid::new_v4().to_string())
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists,
        "addresses remain globally unique"
    );
    let am2 = mailbox(&a, &domain.id, "alice-two", &Uuid::new_v4().to_string())
        .await
        .unwrap();
    let ap = a
        .list_mailboxes(Request::new(ListMailboxesRequest {
            page_size: 1,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!ap.next_page_token.is_empty());
    assert_eq!(
        b.list_mailboxes(Request::new(ListMailboxesRequest {
            page_size: 1,
            page_token: ap.next_page_token.clone()
        }))
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
    let bl = b
        .list_mailboxes(Request::new(ListMailboxesRequest::default()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        bl.mailboxes.iter().map(|m| &m.id).collect::<Vec<_>>(),
        vec![&bm.id]
    );
    assert!(!bl.mailboxes.iter().any(|m| m.id == am.id || m.id == am2.id));
    let local = mailbox(&base, &domain.id, "legacy", &Uuid::new_v4().to_string())
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>("SELECT owner_id FROM mailboxes WHERE id=$1")
            .bind(Uuid::parse_str(&local.id).unwrap())
            .fetch_one(&db)
            .await
            .unwrap(),
        LOCAL_USER
    );
    assert!(
        a.list_messages(Request::new(ListMessagesRequest {
            mailbox_id: local.id.clone(),
            folder: 1,
            ..Default::default()
        }))
        .await
        .is_err(),
        "legacy mail is not inherited"
    );
    assert!(
        sqlx::query("UPDATE mailboxes SET owner_id=$2 WHERE id=$1")
            .bind(Uuid::parse_str(&am.id).unwrap())
            .bind(bob)
            .execute(&db)
            .await
            .is_err(),
        "owner is immutable"
    );
    let request = SubmitMessageRequest {
        from_mailbox_id: am.id.clone(),
        to: vec![bm.address.clone()],
        cc: vec![],
        subject: "Synthetic".into(),
        text_body: "Private".into(),
        idempotency_key: Uuid::new_v4().to_string(),
        source_mailbox_id: None,
        reply_to_entry_id: None,
    };
    assert_eq!(
        b.submit_message(Request::new(request.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let sid = a
        .submit_message(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner()
        .submission
        .unwrap();
    assert_eq!(
        sid.recipients[0].mailbox_id, "",
        "recipient mailbox identity is not disclosed"
    );
    assert_eq!(sid.recipients[0].entry_id, "");
    let retry = a
        .submit_message(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner()
        .submission
        .unwrap();
    assert_eq!(retry.id, sid.id);
    assert_eq!(
        b.get_submission(Request::new(GetSubmissionRequest {
            submission_id: sid.id.clone()
        }))
        .await
        .unwrap_err()
        .code(),
        Code::NotFound
    );
    let entry = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let entries = b
                .list_messages(Request::new(ListMessagesRequest {
                    mailbox_id: bm.id.clone(),
                    folder: 1,
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner()
                .entries;
            if let Some(entry) = entries.into_iter().next() {
                break entry;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        b.get_message(Request::new(GetMessageRequest {
            mailbox_id: bm.id.clone(),
            entry_id: entry.id.clone()
        }))
        .await
        .is_ok()
    );
    for reader in [&a, &admin] {
        assert_eq!(
            reader
                .get_message(Request::new(GetMessageRequest {
                    mailbox_id: bm.id.clone(),
                    entry_id: entry.id.clone()
                }))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        assert_eq!(
            reader
                .get_raw_message(Request::new(GetRawMessageRequest {
                    mailbox_id: bm.id.clone(),
                    entry_id: entry.id.clone()
                }))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        assert_eq!(
            reader
                .list_messages(Request::new(ListMessagesRequest {
                    mailbox_id: bm.id.clone(),
                    folder: 1,
                    ..Default::default()
                }))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
    }
    let read = SetReadStateRequest {
        mailbox_id: bm.id.clone(),
        entry_id: entry.id.clone(),
        is_read: true,
        request_id: Uuid::new_v4().to_string(),
    };
    assert_eq!(
        a.set_read_state(Request::new(read.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        pipeline
            .execute(
                Command::authorized(
                    Actor {
                        user_id: alice,
                        administrator: true
                    },
                    Command::Read(read.encode_to_vec())
                ),
                String::new()
            )
            .await
            .unwrap_err()
            .code(),
        Code::NotFound,
        "consumer does not permit administrator reads"
    );
    let mut reply = request.clone();
    reply.idempotency_key = Uuid::new_v4().to_string();
    reply.source_mailbox_id = Some(bm.id.clone());
    reply.reply_to_entry_id = Some(entry.id.clone());
    assert_eq!(
        a.submit_message(Request::new(reply))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let mut recipient_event = ChangeEvent {
        kind: 3,
        mailbox_id: bm.id.clone(),
        entry_id: entry.id.clone(),
        submission_id: sid.id.clone(),
        ..Default::default()
    };
    assert!(
        a.visible_change(
            &Actor {
                user_id: alice,
                administrator: false
            },
            &mut recipient_event
        )
        .await
        .unwrap()
    );
    assert!(recipient_event.mailbox_id.is_empty() && recipient_event.entry_id.is_empty());
    let mut recipient_event = ChangeEvent {
        kind: 3,
        mailbox_id: bm.id.clone(),
        entry_id: entry.id.clone(),
        submission_id: sid.id.clone(),
        ..Default::default()
    };
    assert!(
        b.visible_change(
            &Actor {
                user_id: bob,
                administrator: false
            },
            &mut recipient_event
        )
        .await
        .unwrap()
    );
    assert!(recipient_event.submission_id.is_empty());
    let mut unrelated = ChangeEvent {
        kind: 2,
        mailbox_id: am.id.clone(),
        ..Default::default()
    };
    assert!(
        !b.visible_change(
            &Actor {
                user_id: bob,
                administrator: false
            },
            &mut unrelated
        )
        .await
        .unwrap()
    );
    let mut global_domain = ChangeEvent {
        kind: 6,
        domain_id: domain.id,
        ..Default::default()
    };
    assert!(
        b.visible_change(
            &Actor {
                user_id: bob,
                administrator: false
            },
            &mut global_domain
        )
        .await
        .unwrap()
    );
    // Exercise the identical MCP scoping path with explicit synthetic principals.
    struct Handler;
    impl mcp::client::McpClientHandler for Handler {}
    for (service, own, foreign) in [
        (a.clone(), am.address.clone(), bm.address.clone()),
        (b.clone(), bm.address.clone(), am.address.clone()),
    ] {
        let stop = tokio_util::sync::CancellationToken::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let app = inbox_server::mcp::InboxMcp::new(service, stop.clone()).router();
        let server_stop = stop.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(server_stop.cancelled_owned())
                .await
                .unwrap()
        });
        let client = mcp::client::McpClient::connect(
            "auth-test",
            mcp::client::McpTransportConfig::StreamableHttp {
                endpoint,
                bearer_token: None,
            },
            Arc::new(Handler),
            mcp::client::McpClientOptions::new("auth-test", "1", Duration::from_secs(5))
                .with_events(),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
        let directory = client
            .read_resource(
                "waltr://mailboxes",
                tokio_util::sync::CancellationToken::new(),
            )
            .await
            .unwrap();
        let directory = directory.raw.to_string();
        assert!(directory.contains(&own));
        assert!(!directory.contains(&foreign));
        for uri in [
            format!("waltr://mailboxes/{foreign}"),
            format!("waltr://mailboxes/{foreign}/inbox"),
            format!("waltr://mailboxes/{foreign}/entries/{}", entry.id),
            format!("waltr://mailboxes/{foreign}/entries/{}/body/1", entry.id),
        ] {
            assert!(
                client
                    .read_resource(&uri, tokio_util::sync::CancellationToken::new())
                    .await
                    .is_err()
            );
        }
        if own == bm.id {
            assert!(
                client
                    .read_resource(
                        &format!("waltr://submissions/{}", sid.id),
                        tokio_util::sync::CancellationToken::new()
                    )
                    .await
                    .is_err()
            );
        }
        let request = mcp::McpEventStreamRequest {
            name: "mail.received".into(),
            arguments: Some(
                serde_json::from_value(
                    serde_json::json!({"inbox":format!("waltr://mailboxes/{foreign}/inbox")}),
                )
                .unwrap(),
            ),
            cursor: None,
            max_age_ms: None,
        };
        let mut events = client
            .stream_events(request, tokio_util::sync::CancellationToken::new())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), events.recv())
                .await
                .unwrap()
                .unwrap()
                .is_err(),
            "no foreign-owner Active frame, watermark, mailbox ID or subscribe ID may escape"
        );
        events.shutdown().await;
        client.shutdown().await.unwrap();
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
    pipeline.shutdown().await;
}
