//! Opt-in HTTP adapter coverage against an exclusive isolated stack.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use inbox_server::{
    mcp::InboxMcp,
    pb::{mail_service_server::MailService as _, *},
    pipeline::Pipeline,
    service::MailService,
};
use mcp::{
    McpEventMessage, McpEventStreamRequest, McpToolCall,
    client::{McpClient, McpClientHandler, McpClientOptions, McpTransportConfig},
};
use serde_json::{Value, json};
use std::{env, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tonic::Request;
use uuid::Uuid;
struct Handler;
impl McpClientHandler for Handler {}
fn cancel() -> CancellationToken {
    CancellationToken::new()
}
fn decoded(r: mcp::McpResourceResult) -> Value {
    serde_json::from_str(r.raw["contents"][0]["text"].as_str().unwrap()).unwrap()
}
async fn invoke(client: &McpClient, name: &str, arguments: Value) -> Value {
    client
        .call_tool(
            McpToolCall {
                name: name.into(),
                arguments,
                progress_token: None,
            },
            cancel(),
        )
        .await
        .unwrap()
        .raw
}
fn watch(inbox: &str, cursor: Option<String>) -> McpEventStreamRequest {
    McpEventStreamRequest {
        name: "mail.received".into(),
        arguments: Some(serde_json::from_value(json!({"inbox":inbox})).unwrap()),
        cursor,
        max_age_ms: None,
    }
}
async fn receive(s: &mut mcp::McpEventStream) -> McpEventMessage {
    tokio::time::timeout(Duration::from_secs(20), s.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn http_discovery_strict_inputs_and_absent_broker_fail_closed() {
    let service = MailService {
        auth: Default::default(),
        principal: None,
        db: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://synthetic:synthetic@127.0.0.1:1/synthetic")
            .unwrap(),
        bucket: Arc::new(
            *s3::Bucket::new(
                "synthetic",
                s3::Region::Custom {
                    region: "synthetic".into(),
                    endpoint: "http://127.0.0.1:1".into(),
                },
                s3::creds::Credentials::new(Some("synthetic"), Some("synthetic"), None, None, None)
                    .unwrap(),
            )
            .unwrap()
            .with_path_style(),
        ),
        domain: "inbox.test".into(),
        pipeline: None,
    };
    let stop = cancel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let app = InboxMcp::new(service, stop.clone()).router();
    let server_stop = stop.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_stop.cancelled_owned())
            .await
            .unwrap();
    });
    let client = McpClient::connect(
        "inbox-test",
        McpTransportConfig::StreamableHttp {
            endpoint,
            bearer_token: None,
        },
        Arc::new(Handler),
        McpClientOptions::new("inbox-test", "1", Duration::from_secs(5)).with_events(),
        cancel(),
    )
    .await
    .unwrap();
    let tools = client.list_tools(cancel()).await.unwrap();
    assert_eq!(
        tools.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
        [
            "send_message",
            "reply_to_message",
            "set_read_state",
            "create_domain",
            "set_domain_policy",
            "create_mailbox"
        ]
    );
    assert_eq!(
        client.list_resources(cancel()).await.unwrap()[0].uri,
        "waltr://mailboxes"
    );
    assert_eq!(
        client
            .list_resource_templates(cancel())
            .await
            .unwrap()
            .len(),
        9
    );
    assert_eq!(
        client.list_events(None, cancel()).await.unwrap().events[0].delivery,
        Some(vec!["push".into()])
    );
    assert!(
        client
            .read_resource("waltr://mailboxes?limit=1&limit=2", cancel())
            .await
            .is_err()
    );
    let from = format!("waltr://mailboxes/{}@inbox.test", Uuid::new_v4());
    let args = json!({"from":from,"to":["synthetic@inbox.test"],"cc":[],"subject":"Synthetic","body":"Synthetic","idempotency_key":"stable"});
    let result = client
        .call_tool(
            McpToolCall {
                name: "send_message".into(),
                arguments: args.clone(),
                progress_token: None,
            },
            cancel(),
        )
        .await
        .unwrap();
    assert_eq!(result.raw["structuredContent"]["outcome"], "unknown");
    assert_eq!(result.raw["structuredContent"]["key"], "stable");
    let domain = format!("waltr://domains/{}", Uuid::new_v4());
    for (name, arguments) in [
        (
            "create_domain",
            json!({"name":"synthetic.test","receive_enabled":false,"send_enabled":true,"request_id":"domain-key"}),
        ),
        (
            "set_domain_policy",
            json!({"domain":domain,"receive_enabled":false,"send_enabled":false,"request_id":"domain-key"}),
        ),
        (
            "create_mailbox",
            json!({"domain":domain,"local_part":"synthetic","name":"Synthetic","request_id":"domain-key"}),
        ),
    ] {
        let r = invoke(&client, name, arguments.clone()).await;
        assert_eq!(r["structuredContent"]["outcome"], "unknown");
        assert_eq!(r["structuredContent"]["retry_same_key"], true);
        assert_eq!(r["structuredContent"]["key"], "domain-key");
        for bad in [
            {
                let mut a = arguments.clone();
                a["extra"] = json!(true);
                a
            },
            {
                let mut a = arguments.clone();
                a["request_id"] = json!("");
                a
            },
            {
                let mut a = arguments.clone();
                a.as_object_mut().unwrap().remove("request_id");
                a
            },
        ] {
            assert!(
                client
                    .call_tool(
                        McpToolCall {
                            name: name.into(),
                            arguments: bad,
                            progress_token: None
                        },
                        cancel()
                    )
                    .await
                    .is_err()
            );
        }
    }
    let mut unknown = args;
    unknown["internet_message_id"] = json!("forbidden");
    assert!(
        client
            .call_tool(
                McpToolCall {
                    name: "send_message".into(),
                    arguments: unknown,
                    progress_token: None
                },
                cancel()
            )
            .await
            .is_err()
    );
    let mut request = watch(&format!("{from}/inbox"), None);
    request.max_age_ms = Some(0);
    let mut stream = client.stream_events(request, cancel()).await.unwrap();
    assert!(stream.recv().await.unwrap().is_err());
    stream.shutdown().await;
    client.shutdown().await.unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires exclusive isolated INBOX_TEST_DATABASE_URL, INBOX_TEST_NATS_URL and INBOX_TEST_S3_*; retains synthetic fixtures"]
async fn http_resources_mutations_events_replay_and_cancel() {
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
    let mut mailboxes = vec![];
    for _ in 0..2 {
        mailboxes.push(
            service
                .create_mailbox(Request::new(CreateMailboxRequest {
                    local_part: format!("mcp-{}", Uuid::new_v4()),
                    name: "Synthetic MCP test".into(),
                    request_id: Uuid::new_v4().to_string(),
                    domain_id: String::new(),
                }))
                .await
                .unwrap()
                .into_inner()
                .mailbox
                .unwrap(),
        );
    }
    let stop = cancel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let app = InboxMcp::new(service.clone(), stop.clone()).router();
    let server_stop = stop.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_stop.cancelled_owned())
            .await
            .unwrap();
    });
    let client = McpClient::connect(
        "inbox-test",
        McpTransportConfig::StreamableHttp {
            endpoint,
            bearer_token: None,
        },
        Arc::new(Handler),
        McpClientOptions::new("inbox-test", "1", Duration::from_secs(30)).with_events(),
        cancel(),
    )
    .await
    .unwrap();
    assert_eq!(client.list_tools(cancel()).await.unwrap().len(), 6);
    assert!(!client.list_resources(cancel()).await.unwrap().is_empty());
    assert_eq!(
        client
            .list_resource_templates(cancel())
            .await
            .unwrap()
            .len(),
        9
    );
    assert_eq!(
        client.list_events(None, cancel()).await.unwrap().events[0].name,
        "mail.received"
    );
    let from = format!("waltr://mailboxes/{}", mailboxes[0].address);
    let inbox = format!("waltr://mailboxes/{}/inbox", mailboxes[1].address);
    let sender_inbox = format!("{from}/inbox");
    let sender_before =
        inbox_server::arrivals::load_watermark(&db, mailboxes[0].id.parse().unwrap())
            .await
            .unwrap();
    let mut stream = client
        .stream_events(watch(&inbox, None), cancel())
        .await
        .unwrap();
    let initial = match receive(&mut stream).await {
        McpEventMessage::Active {
            cursor: Some(c),
            truncated: false,
        } => c,
        m => panic!("unexpected initial message {m:?}"),
    };
    let domain_args = json!({"name":format!("mcp-{}.test",Uuid::new_v4()),"receive_enabled":false,"send_enabled":false,"request_id":Uuid::new_v4().to_string()});
    let created = invoke(&client, "create_domain", domain_args.clone()).await;
    assert_eq!(created["structuredContent"]["outcome"], "committed");
    assert_eq!(created["content"][1]["type"], "resource_link");
    let d = &created["structuredContent"]["domain"];
    let domain = d["uri"].as_str().unwrap();
    assert_eq!(
        decoded(client.read_resource(domain, cancel()).await.unwrap()),
        *d
    );
    let retried = invoke(&client, "create_domain", domain_args.clone()).await;
    assert_eq!(retried["structuredContent"], created["structuredContent"]);
    let mut duplicate = domain_args;
    duplicate["request_id"] = json!(Uuid::new_v4().to_string());
    assert_eq!(
        invoke(&client, "create_domain", duplicate).await["structuredContent"]["outcome"],
        "not_accepted"
    );
    let directory = decoded(
        client
            .read_resource("waltr://domains?limit=1", cancel())
            .await
            .unwrap(),
    );
    assert_eq!(directory["domains"].as_array().unwrap().len(), 1);
    let next = directory["next"].as_str().unwrap();
    assert!(client.read_resource(next, cancel()).await.is_ok());
    assert!(
        client
            .read_resource(&next.replace("limit=1", "limit=2"), cancel())
            .await
            .is_err()
    );
    let mailbox_args = json!({"domain":domain,"local_part":mailboxes[0].local_part,"name":"Synthetic domain mailbox","request_id":Uuid::new_v4().to_string()});
    let managed = invoke(&client, "create_mailbox", mailbox_args.clone()).await;
    assert_eq!(managed["structuredContent"]["outcome"], "committed");
    assert_eq!(managed["content"][2]["uri"], domain);
    let m = &managed["structuredContent"]["mailbox"];
    assert_eq!(m["domain"], domain);
    assert_eq!(m["domain_id"], d["id"]);
    assert_eq!(m["domain_name"], d["name"]);
    assert_eq!(
        decoded(
            client
                .read_resource(m["uri"].as_str().unwrap(), cancel())
                .await
                .unwrap()
        ),
        *m
    );
    assert_eq!(
        invoke(&client, "create_mailbox", mailbox_args.clone()).await["structuredContent"],
        managed["structuredContent"]
    );
    let mut duplicate = mailbox_args;
    duplicate["request_id"] = json!(Uuid::new_v4().to_string());
    assert_eq!(
        invoke(&client, "create_mailbox", duplicate).await["structuredContent"]["outcome"],
        "not_accepted"
    );
    let rejected = invoke(&client,"send_message",json!({"from":m["uri"],"to":[mailboxes[1].address],"cc":[],"subject":"Synthetic","body":"Synthetic","idempotency_key":Uuid::new_v4().to_string()})).await;
    assert_eq!(rejected["structuredContent"]["outcome"], "not_accepted");
    assert_eq!(rejected["structuredContent"]["retry_same_key"], false);
    let policy = json!({"domain":domain,"receive_enabled":true,"send_enabled":false,"request_id":Uuid::new_v4().to_string()});
    let changed = invoke(&client, "set_domain_policy", policy.clone()).await;
    assert_eq!(changed["structuredContent"]["outcome"], "committed");
    assert_eq!(
        changed["structuredContent"]["domain"]["receive_enabled"],
        true
    );
    assert_eq!(
        changed["structuredContent"]["domain"]["send_enabled"],
        false
    );
    assert_eq!(
        invoke(&client, "set_domain_policy", policy).await["structuredContent"],
        changed["structuredContent"]
    );
    let unknown_domain = format!("waltr://domains/{}", Uuid::new_v4());
    for (name, args) in [
        (
            "set_domain_policy",
            json!({"domain":unknown_domain,"receive_enabled":true,"send_enabled":true,"request_id":Uuid::new_v4().to_string()}),
        ),
        (
            "create_mailbox",
            json!({"domain":unknown_domain,"local_part":"synthetic","name":"Synthetic","request_id":Uuid::new_v4().to_string()}),
        ),
    ] {
        let result = invoke(&client, name, args).await;
        assert_eq!(result["structuredContent"]["outcome"], "not_accepted");
        assert_eq!(result["structuredContent"]["retry_same_key"], false);
    }
    let args = json!({"from":from,"to":[mailboxes[1].address],"cc":[],"subject":"Synthetic MCP","body":"a😀".repeat(20000),"idempotency_key":Uuid::new_v4().to_string()});
    let sent = client
        .call_tool(
            McpToolCall {
                name: "send_message".into(),
                arguments: args.clone(),
                progress_token: None,
            },
            cancel(),
        )
        .await
        .unwrap();
    assert_eq!(sent.raw["structuredContent"]["outcome"], "accepted");
    let bootstrap = format!("waltr://domains/{}", mailboxes[0].domain_id);
    let disabled = invoke(&client,"set_domain_policy",json!({"domain":bootstrap,"receive_enabled":false,"send_enabled":false,"request_id":Uuid::new_v4().to_string()})).await;
    assert_eq!(disabled["structuredContent"]["outcome"], "committed");
    let retry = client
        .call_tool(
            McpToolCall {
                name: "send_message".into(),
                arguments: args.clone(),
                progress_token: None,
            },
            cancel(),
        )
        .await
        .unwrap();
    assert_eq!(
        sent.raw["structuredContent"]["submission"]["id"],
        retry.raw["structuredContent"]["submission"]["id"]
    );
    let mut new_send = args;
    new_send["idempotency_key"] = json!(Uuid::new_v4().to_string());
    assert_eq!(
        invoke(&client, "send_message", new_send).await["structuredContent"]["outcome"],
        "not_accepted"
    );
    assert!(client.read_resource(&from, cancel()).await.is_ok());
    let enabled = invoke(&client,"set_domain_policy",json!({"domain":bootstrap,"receive_enabled":true,"send_enabled":true,"request_id":Uuid::new_v4().to_string()})).await;
    assert_eq!(enabled["structuredContent"]["outcome"], "committed");
    let (event_id, message, event_cursor) = loop {
        match receive(&mut stream).await {
            McpEventMessage::Event {
                event_id,
                data,
                cursor: Some(c),
                ..
            } => {
                assert_eq!(data.as_object().unwrap().len(), 2);
                assert_eq!(data["inbox"], inbox);
                break (event_id, data["message"].as_str().unwrap().to_owned(), c);
            }
            _ => continue,
        }
    };
    let mut read = decoded(client.read_resource(&message, cancel()).await.unwrap());
    assert_eq!(read["entry"]["is_read"], false);
    let mut body = read["text_body"].as_str().unwrap().to_owned();
    while let Some(uri) = read["next"].as_str() {
        read = decoded(client.read_resource(uri, cancel()).await.unwrap());
        body.push_str(read["text_body"].as_str().unwrap());
    }
    assert_eq!(body, "a😀".repeat(20000));
    assert_eq!(
        decoded(client.read_resource(&message, cancel()).await.unwrap())["entry"]["is_read"],
        false
    );
    let read_key = Uuid::new_v4().to_string();
    let marked = client
        .call_tool(
            McpToolCall {
                name: "set_read_state".into(),
                arguments: json!({"entry":message,"is_read":true,"request_id":read_key}),
                progress_token: None,
            },
            cancel(),
        )
        .await
        .unwrap();
    assert_eq!(marked.raw["structuredContent"]["outcome"], "committed");
    let replay = client
        .stream_events(watch(&inbox, Some(initial.clone())), cancel())
        .await
        .unwrap();
    let mut replay = replay;
    assert_eq!(
        receive(&mut replay).await,
        McpEventMessage::Active {
            cursor: Some(initial),
            truncated: false
        }
    );
    match receive(&mut replay).await {
        McpEventMessage::Event {
            event_id: id,
            cursor: Some(c),
            ..
        } => {
            assert_eq!(id, event_id);
            assert_eq!(c, event_cursor)
        }
        m => panic!("unexpected replay {m:?}"),
    }
    let actor = service.principal(&Request::new(())).unwrap().actor;
    let inner_event_cursor = service
        .auth
        .unbind_cursor(pipeline.namespace(), &actor, "arrival", &event_cursor)
        .unwrap();
    let page = inbox_server::arrivals::read_after(
        &db,
        mailboxes[1].id.parse().unwrap(),
        &inner_event_cursor,
        100,
    )
    .await
    .unwrap();
    assert!(page.arrivals.is_empty());
    assert!(
        inbox_server::arrivals::read_after(
            &db,
            mailboxes[0].id.parse().unwrap(),
            sender_before.as_str(),
            100
        )
        .await
        .unwrap()
        .arrivals
        .is_empty()
    );
    assert!(
        decoded(client.read_resource(&sender_inbox, cancel()).await.unwrap())["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let job = sent.raw["structuredContent"]["submission"]["uri"]
        .as_str()
        .unwrap();
    assert!(decoded(client.read_resource(job, cancel()).await.unwrap())["id"].is_string());
    let identity = decoded(client.read_resource(&from, cancel()).await.unwrap());
    assert_eq!(identity["sent"], format!("{from}/sent"));
    let sent_entries = decoded(
        client
            .read_resource(&format!("{from}/sent?limit=1"), cancel())
            .await
            .unwrap(),
    );
    assert_eq!(sent_entries["entries"].as_array().unwrap().len(), 1);
    let wrong = message.replace(&mailboxes[1].address, &mailboxes[0].address);
    assert!(client.read_resource(&wrong, cancel()).await.is_err());
    let other_actor = inbox_server::auth::Actor {
        user_id: Uuid::new_v4(),
        administrator: false,
    };
    for invalid in [
        format!("{event_cursor}x"),
        inner_event_cursor.clone(),
        service.auth.bind_cursor(
            &format!("{}_FOREIGN", pipeline.namespace()),
            &actor,
            "arrival",
            &inner_event_cursor,
        ),
        service.auth.bind_cursor(
            pipeline.namespace(),
            &other_actor,
            "arrival",
            &inner_event_cursor,
        ),
    ] {
        let mut rejected = client
            .stream_events(watch(&inbox, Some(invalid)), cancel())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), rejected.recv())
                .await
                .unwrap()
                .unwrap()
                .is_err(),
            "malformed, unwrapped, foreign-namespace and foreign-principal public cursors must reject before ledger replay"
        );
        rejected.shutdown().await;
    }
    let mut future: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&inner_event_cursor).unwrap()).unwrap();
    future["position"] = json!(i64::MAX);
    let inner_token = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&future).unwrap());
    let token = service
        .auth
        .bind_cursor(pipeline.namespace(), &actor, "arrival", &inner_token);
    let mut reset = client
        .stream_events(watch(&inbox, Some(token)), cancel())
        .await
        .unwrap();
    assert!(matches!(
        receive(&mut reset).await,
        McpEventMessage::Active {
            truncated: true,
            ..
        }
    ));
    reset.shutdown().await;
    let mut foreign = client
        .stream_events(watch(&sender_inbox, Some(event_cursor)), cancel())
        .await
        .unwrap();
    assert!(foreign.recv().await.unwrap().is_err());
    foreign.shutdown().await;
    let replied=client.call_tool(McpToolCall {name:"reply_to_message".into(),arguments:json!({"from":format!("waltr://mailboxes/{}",mailboxes[1].address),"source":message,"to":[mailboxes[0].address],"cc":[],"subject":"Re: Synthetic MCP","body":"Synthetic reply","idempotency_key":Uuid::new_v4().to_string()}),progress_token:None},cancel()).await.unwrap();
    assert_eq!(replied.raw["structuredContent"]["outcome"], "accepted");
    let replied_view = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let page = decoded(client.read_resource(&sender_inbox, cancel()).await.unwrap());
            if let Some(uri) = page["entries"][0]["uri"].as_str() {
                break decoded(client.read_resource(uri, cancel()).await.unwrap());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let original = decoded(client.read_resource(&message, cancel()).await.unwrap());
    assert_eq!(replied_view["in_reply_to"], original["internet_message_id"]);
    assert_eq!(
        replied_view["references"]
            .as_array()
            .unwrap()
            .last()
            .unwrap(),
        &original["internet_message_id"]
    );
    replay.shutdown().await;
    stream.shutdown().await;
    client.shutdown().await.unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    pipeline.shutdown().await;
    db.close().await;
}
