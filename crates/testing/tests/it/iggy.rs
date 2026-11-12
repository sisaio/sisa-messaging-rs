use iggy::prelude::{StreamClient, StreamCreateOptions};
use sisa_eventing_testing::Iggy;

#[tokio::test]
async fn iggy_starts_and_takes_a_stream() {
    let iggy = Iggy::start().await;

    iggy.client()
        .await
        .create_stream("started", &StreamCreateOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_restarted_iggy_keeps_its_address_and_streams() {
    let iggy = Iggy::start_restartable().await;
    iggy.client()
        .await
        .create_stream("kept", &StreamCreateOptions::default())
        .await
        .unwrap();

    iggy.stop().await;
    iggy.restart().await;

    let stream = iggy
        .client()
        .await
        .get_stream(&"kept".try_into().unwrap())
        .await
        .unwrap();
    assert!(stream.is_some());
}
