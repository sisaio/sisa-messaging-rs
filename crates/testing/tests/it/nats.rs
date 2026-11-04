use sisa_messaging_testing::Nats;

#[tokio::test]
async fn nats_starts_with_jetstream_on() {
    let nats = Nats::start().await;

    nats.jetstream().await.query_account().await.unwrap();
}

#[tokio::test]
async fn a_restarted_nats_keeps_its_address_and_streams() {
    let nats = Nats::start_restartable().await;
    let stream = async_nats::jetstream::stream::Config {
        name: "KEPT".into(),
        subjects: vec!["kept.>".into()],
        ..Default::default()
    };
    nats.jetstream().await.create_stream(stream).await.unwrap();

    nats.stop().await;
    assert!(async_nats::connect(nats.url()).await.is_err());
    nats.restart().await;

    nats.jetstream().await.get_stream("KEPT").await.unwrap();
}
