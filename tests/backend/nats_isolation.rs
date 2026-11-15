use futures_util::StreamExt;
use inbox_server::{nats_config::Config, pb::ChangeEvent, pipeline::Pipeline};
use prost::Message;
use std::{collections::HashMap, time::Duration};
use uuid::Uuid;

fn config(namespace: &str, replicas: &str) -> Config {
    let values = HashMap::from([
        ("INBOX_NATS_NAMESPACE".into(), namespace.into()),
        ("INBOX_NATS_REPLICAS".into(), replicas.into()),
    ]);
    Config::from_values(values, true).unwrap()
}

#[tokio::test]
#[ignore = "requires explicitly private fresh INBOX_TEST_NATS_ISOLATION_URL and INBOX_TEST_NATS_ISOLATION_REPLICAS; retains synthetic streams"]
async fn two_namespaces_isolate_commands_results_changes_staging_and_cursors() {
    let url = std::env::var("INBOX_TEST_NATS_ISOLATION_URL").expect("private fresh broker URL");
    let replicas = std::env::var("INBOX_TEST_NATS_ISOLATION_REPLICAS")
        .expect("explicit replica count supported by private broker");
    let run = Uuid::new_v4().simple().to_string().to_uppercase();
    let a = config(&format!("INBOX_TEST_A_{run}"), &replicas);
    let b = config(&format!("INBOX_TEST_B_{run}"), &replicas);
    let pa = Pipeline::connect_configured(&url, a.clone()).await.unwrap();
    let pb = Pipeline::connect_configured(&url, b.clone()).await.unwrap();
    let js = async_nats::jetstream::new(a.connect(&url).await.unwrap());

    for c in [&a, &b] {
        for suffix in ["COMMANDS", "RESULTS", "CHANGES", "STAGING"] {
            let name = if suffix == "STAGING" {
                format!("OBJ_{}", c.resource(suffix))
            } else {
                c.resource(suffix)
            };
            let stream = js.get_stream(name).await.unwrap();
            assert_eq!(stream.cached_info().config.num_replicas, c.replicas());
            if suffix == "STAGING" {
                assert_eq!(
                    stream.cached_info().config.subjects,
                    vec![
                        format!("$O.{}.C.>", c.resource("STAGING")),
                        format!("$O.{}.M.>", c.resource("STAGING")),
                    ]
                );
            }
        }
    }

    let id = Uuid::new_v4();
    for c in [&a, &b] {
        js.publish(
            c.subject("commands.api"),
            c.namespace().as_bytes().to_vec().into(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
        js.publish(
            c.subject(&format!("results.{id}")),
            c.namespace().as_bytes().to_vec().into(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
        let objects = js.get_object_store(c.resource("STAGING")).await.unwrap();
        objects
            .put(c.namespace(), &mut c.namespace().as_bytes())
            .await
            .unwrap();
    }

    for (c, other) in [(&a, &b), (&b, &a)] {
        let stream = js.get_stream(c.resource("COMMANDS")).await.unwrap();
        let consumer = stream
            .get_or_create_consumer(
                &c.resource("MUTATIONS"),
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(c.resource("MUTATIONS")),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        // Redelivery/replay never changes the stream's environment.
        for _ in 0..2 {
            let mut messages = consumer
                .batch()
                .max_messages(1)
                .expires(Duration::from_secs(2))
                .messages()
                .await
                .unwrap();
            let message = messages.next().await.unwrap().unwrap();
            assert_eq!(message.payload.as_ref(), c.namespace().as_bytes());
            message
                .ack_with(async_nats::jetstream::AckKind::Nak(None))
                .await
                .unwrap();
        }
        let results = js.get_stream(c.resource("RESULTS")).await.unwrap();
        let result = results
            .get_last_raw_message_by_subject(&c.subject(&format!("results.{id}")))
            .await
            .unwrap();
        assert_eq!(result.payload.as_ref(), c.namespace().as_bytes());
        assert!(
            results
                .get_last_raw_message_by_subject(&other.subject(&format!("results.{id}")))
                .await
                .is_err()
        );
        let objects = js.get_object_store(c.resource("STAGING")).await.unwrap();
        assert!(objects.get(other.namespace()).await.is_err());
    }

    let mut wa = pa.watch(String::new()).await.unwrap();
    let mut wb = pb.watch(String::new()).await.unwrap();
    let ca = wa.next().await.unwrap().unwrap();
    let cb = wb.next().await.unwrap().unwrap();
    assert_eq!(ca.kind, 1);
    assert_eq!(cb.kind, 1);
    assert_ne!(ca.resume_token, cb.resume_token);
    let mut cross = pb.watch(ca.resume_token.clone()).await.unwrap();
    assert_eq!(cross.next().await.unwrap().unwrap().kind, 1);
    let mut same = pa.watch(ca.resume_token).await.unwrap();
    assert_eq!(same.next().await.unwrap().unwrap().kind, 5);
    for (c, kind) in [(&a, 2), (&b, 3)] {
        js.publish(
            c.subject("changes"),
            ChangeEvent {
                kind,
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
    for (watch, kind) in [(&mut wa, 2), (&mut wb, 3)] {
        let event = tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.kind, kind);
    }

    // Reusing a namespace with another replica policy must not update existing streams.
    let wrong = config(a.namespace(), if replicas == "1" { "3" } else { "1" });
    assert!(Pipeline::connect_configured(&url, wrong).await.is_err());
    let commands = js.get_stream(a.resource("COMMANDS")).await.unwrap();
    assert_eq!(commands.cached_info().config.num_replicas, a.replicas());
    pa.shutdown().await;
    pb.shutdown().await;
}
